use crate::reference::Reference;
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

/// Flexible access field that can be either a string or an array
/// to handle both old and new JWT token formats from registry providers
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are part of JWT API structure, may be used for future features
pub enum AccessField {
    String(String),
    Array(Vec<AccessEntry>),
}

#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Fields are part of JWT access structure, may be used for future features
pub struct AccessEntry {
    #[serde(rename = "type")]
    pub access_type: String,
    pub name: String,
    pub actions: Vec<String>,
}

impl<'de> Deserialize<'de> for AccessField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // First try to deserialize as a string
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum AccessFieldHelper {
            String(String),
            Array(Vec<AccessEntry>),
        }

        match AccessFieldHelper::deserialize(deserializer)? {
            AccessFieldHelper::String(s) => Ok(AccessField::String(s)),
            AccessFieldHelper::Array(a) => Ok(AccessField::Array(a)),
        }
    }
}

/// Custom JWT claims structure that handles flexible access field
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Fields are part of JWT claims structure, may be used for future features
pub struct FlexibleJwtClaims {
    #[serde(rename = "exp")]
    pub expiration: Option<u64>,
    #[serde(rename = "iat")]
    pub issued_at: Option<u64>,
    #[serde(rename = "sub")]
    pub subject: Option<String>,
    #[serde(rename = "aud")]
    pub audience: Option<Vec<String>>,
    #[serde(rename = "iss")]
    pub issuer: Option<String>,
    #[serde(rename = "jti")]
    pub jti: Option<String>,
    #[serde(rename = "nbf")]
    pub not_before: Option<u64>,
    pub access: Option<AccessField>,
}

/// A token granted during the OAuth2-like workflow for OCI registries.
#[derive(Deserialize, Clone)]
#[serde(untagged)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RegistryToken {
    Token { token: String },
    AccessToken { access_token: String },
}

impl fmt::Debug for RegistryToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted = String::from("<redacted>");
        match self {
            RegistryToken::Token { .. } => {
                f.debug_struct("Token").field("token", &redacted).finish()
            }
            RegistryToken::AccessToken { .. } => f
                .debug_struct("AccessToken")
                .field("access_token", &redacted)
                .finish(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum RegistryTokenType {
    Bearer(RegistryToken),
    Basic(String, String),
}

impl RegistryToken {
    pub fn bearer_token(&self) -> String {
        format!("Bearer {}", self.token())
    }

    pub fn token(&self) -> &str {
        match self {
            RegistryToken::Token { token } => token,
            RegistryToken::AccessToken { access_token } => access_token,
        }
    }
}

/// Desired operation for registry authentication
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum RegistryOperation {
    /// Authenticate for push operations
    Push,
    /// Authenticate for pull operations
    Pull,
}

#[derive(Default)]
pub(crate) struct TokenCache {
    // (registry, repository, scope) -> (token, expiration)
    tokens: BTreeMap<(String, String, RegistryOperation), (RegistryTokenType, u64)>,
}

impl TokenCache {
    pub(crate) fn new() -> Self {
        TokenCache {
            tokens: BTreeMap::new(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        reference: &Reference,
        op: RegistryOperation,
        token: RegistryTokenType,
    ) {
        let expiration = match token {
            RegistryTokenType::Basic(_, _) => u64::MAX,
            RegistryTokenType::Bearer(ref t) => {
                let token_str = t.token();
                match jwt::Token::<
                        jwt::header::Header,
                        FlexibleJwtClaims,
                        jwt::token::Unverified,
                    >::parse_unverified(token_str)
                    {
                        Ok(token) => {
                            debug!("Successfully parsed JWT token with flexible claims");
                            token.claims().expiration.unwrap_or(u64::MAX)
                        },
                        Err(jwt::Error::NoClaimsComponent) => {
                            debug!(?token, "Cannot extract expiration from token's claims, assuming forever");
                            u64::MAX
                        },
                        Err(error) => {
                            // If flexible parsing fails, try to extract expiration manually
                            warn!(?error, "Failed to parse JWT with flexible claims, trying manual extraction");
                            match extract_expiration_manually(token_str) {
                                Some(exp) => {
                                    debug!("Successfully extracted expiration manually: {}", exp);
                                    exp
                                },
                                None => {
                                    warn!("Failed to extract expiration manually, assuming forever");
                                    u64::MAX
                                }
                            }
                        }
                    }
            }
        };
        let registry = reference.resolve_registry().to_string();
        let repository = reference.repository().to_string();
        debug!(%registry, %repository, ?op, %expiration, "Inserting token");
        self.tokens
            .insert((registry, repository, op), (token, expiration));
    }

    pub(crate) fn get(
        &self,
        reference: &Reference,
        op: RegistryOperation,
    ) -> Option<&RegistryTokenType> {
        let registry = reference.resolve_registry().to_string();
        let repository = reference.repository().to_string();
        match self.tokens.get(&(registry.clone(), repository.clone(), op)) {
            Some((ref token, expiration)) => {
                let now = SystemTime::now();
                let epoch = now
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs();
                if epoch > *expiration {
                    debug!(%registry, %repository, ?op, %expiration, miss=false, expired=true, "Fetching token");
                    None
                } else {
                    debug!(%registry, %repository, ?op, %expiration, miss=false, expired=false, "Fetching token");
                    Some(token)
                }
            }
            None => {
                debug!(%registry, %repository, ?op, miss=true, "Fetching token");
                None
            }
        }
    }

    pub(crate) fn contains_key(&self, reference: &Reference, op: RegistryOperation) -> bool {
        self.get(reference, op).is_some()
    }
}

/// Manual JWT expiration extraction as fallback when structured parsing fails
fn extract_expiration_manually(token: &str) -> Option<u64> {
    // JWT format: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        warn!("Invalid JWT format: expected 3 parts, got {}", parts.len());
        return None;
    }

    // Decode the payload (base64url)
    let payload = parts[1];
    match base64_url_decode(payload) {
        Ok(decoded) => match serde_json::from_slice::<serde_json::Value>(&decoded) {
            Ok(claims) => {
                if let Some(exp) = claims.get("exp") {
                    if let Some(exp_num) = exp.as_u64() {
                        debug!("Extracted expiration from JWT payload: {}", exp_num);
                        return Some(exp_num);
                    } else if let Some(exp_f64) = exp.as_f64() {
                        let exp_u64 = exp_f64 as u64;
                        debug!(
                            "Extracted expiration from JWT payload (converted from f64): {}",
                            exp_u64
                        );
                        return Some(exp_u64);
                    }
                }
                debug!("No 'exp' field found in JWT claims");
                None
            }
            Err(e) => {
                warn!("Failed to parse JWT payload as JSON: {}", e);
                None
            }
        },
        Err(e) => {
            warn!("Failed to decode JWT payload: {}", e);
            None
        }
    }
}

/// Base64 URL decode (without padding)
fn base64_url_decode(input: &str) -> Result<Vec<u8>, String> {
    use base64::{engine::general_purpose, Engine as _};

    // Add padding if needed
    let mut padded = input.to_string();
    while !padded.len().is_multiple_of(4) {
        padded.push('=');
    }

    // Replace URL-safe characters
    let standard = padded
        .chars()
        .map(|c| match c {
            '-' => '+',
            '_' => '/',
            c => c,
        })
        .collect::<String>();

    general_purpose::STANDARD
        .decode(&standard)
        .map_err(|e| format!("Base64 decode error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flexible_jwt_claims_array_format() {
        // Test Replicated's actual JWT format
        let replicated_jwt_payload = r#"{
            "access": [{"type":"repository","name":"proxy/wallaroo/ghcr.io/wallaroolabs/fitzroy-mini","actions":["pull"]}],
            "exp": 1774988620
        }"#;

        let claims: FlexibleJwtClaims = serde_json::from_str(replicated_jwt_payload).unwrap();
        assert_eq!(claims.expiration, Some(1774988620));

        if let Some(AccessField::Array(entries)) = claims.access {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].access_type, "repository");
            assert_eq!(
                entries[0].name,
                "proxy/wallaroo/ghcr.io/wallaroolabs/fitzroy-mini"
            );
            assert_eq!(entries[0].actions, vec!["pull"]);
        } else {
            panic!("Expected AccessField::Array");
        }
    }

    #[test]
    fn test_flexible_jwt_claims_string_format() {
        // Test legacy string format
        let legacy_jwt_payload = r#"{
            "access": "some-string-format",
            "exp": 1774988620
        }"#;

        let claims: FlexibleJwtClaims = serde_json::from_str(legacy_jwt_payload).unwrap();
        assert_eq!(claims.expiration, Some(1774988620));

        if let Some(AccessField::String(access_str)) = claims.access {
            assert_eq!(access_str, "some-string-format");
        } else {
            panic!("Expected AccessField::String");
        }
    }

    #[test]
    fn test_manual_jwt_extraction() {
        // Real JWT token with array access field
        let test_jwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhY2Nlc3MiOlt7InR5cGUiOiJyZXBvc2l0b3J5IiwibmFtZSI6InByb3h5L3dhbGxhcm9vL2doci5pby93YWxsYXJvb2xhYnMvZml0enJveS1taW5pIiwiYWN0aW9ucyI6WyJwdWxsIl19XSwiZXhwIjoxNzc0OTg4NjIwfQ.signature";

        let exp = extract_expiration_manually(test_jwt).unwrap();
        assert_eq!(exp, 1774988620);
    }
}
