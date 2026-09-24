//! `relayers(address)` decoding for both NoxRegistry layouts.
//!
//! - April 2026 registry (non-proxy): 7 fields
//!   `(bytes32 sphinxKey, string url, string ingressUrl, string metadataUrl,
//!     uint256 stakedAmount, uint256 unstakeRequestTime, bool isRegistered)`
//! - UUPS registry (paid execution): 9 fields, adding
//!   `(uint8 status, bool frozen)`
//!
//! The layouts cannot be told apart by trial decoding: a 7-field decode of
//! 9-field data succeeds and silently drops `status`/`frozen`. They are told
//! apart by the head offset of the first string, which is the byte size of the
//! static head: `7 * 32 = 0xe0` or `9 * 32 = 0x120`.

use ethers::abi::{self, ParamType, Token};
use ethers::types::{Address, Bytes, U256};
use ethers::utils::keccak256;

const WORD: usize = 32;
const LEGACY_FIELDS: usize = 7;
const CURRENT_FIELDS: usize = 9;

/// On-chain `RelayerStatus` of the UUPS registry.
pub const STATUS_NONE: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileLayout {
    Legacy7,
    Current9,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayerProfile {
    pub sphinx_key: [u8; 32],
    pub url: String,
    pub ingress_url: String,
    pub metadata_url: String,
    pub staked_amount: U256,
    /// `unstakeRequestTime` (legacy) or `unlockTime` (current).
    pub unstake_time: U256,
    pub is_registered: bool,
    /// `None` on the legacy layout, which has no status field.
    pub status: Option<u8>,
    pub frozen: bool,
    pub layout: ProfileLayout,
}

/// Calldata for `relayers(address)`.
pub fn relayers_calldata(address: Address) -> Bytes {
    let selector = &keccak256(b"relayers(address)")[..4];
    let mut data = selector.to_vec();
    data.extend(abi::encode(&[Token::Address(address)]));
    data.into()
}

fn param_types(layout: ProfileLayout) -> Vec<ParamType> {
    let mut types = vec![
        ParamType::FixedBytes(32),
        ParamType::String,
        ParamType::String,
        ParamType::String,
        ParamType::Uint(256),
        ParamType::Uint(256),
        ParamType::Bool,
    ];
    if layout == ProfileLayout::Current9 {
        types.push(ParamType::Uint(8));
        types.push(ParamType::Bool);
    }
    types
}

fn detect_layout(data: &[u8]) -> Result<ProfileLayout, String> {
    if data.len() < 2 * WORD {
        return Err(format!(
            "relayers() returned {} bytes, too short for a profile",
            data.len()
        ));
    }
    let offset = U256::from_big_endian(&data[WORD..2 * WORD]);
    if offset == U256::from(LEGACY_FIELDS * WORD) {
        Ok(ProfileLayout::Legacy7)
    } else if offset == U256::from(CURRENT_FIELDS * WORD) {
        Ok(ProfileLayout::Current9)
    } else {
        Err(format!(
            "relayers() returned an unknown layout (first string offset {offset:#x})"
        ))
    }
}

/// Decode the raw return data of `relayers(address)`.
pub fn decode_relayer_profile(data: &[u8]) -> Result<RelayerProfile, String> {
    let layout = detect_layout(data)?;
    let tokens = abi::decode(&param_types(layout), data)
        .map_err(|e| format!("relayers() {layout:?} decode failed: {e}"))?;
    let mut fields = tokens.into_iter();
    let mut next = |name: &str| {
        fields
            .next()
            .ok_or_else(|| format!("relayers() is missing field {name}"))
    };

    let sphinx_key = match next("sphinxKey")? {
        Token::FixedBytes(bytes) if bytes.len() == 32 => {
            let mut key = [0_u8; 32];
            key.copy_from_slice(&bytes);
            key
        }
        other => {
            return Err(format!(
                "relayers() sphinxKey has unexpected token {other:?}"
            ))
        }
    };
    let string = |token: Token, name: &str| match token {
        Token::String(value) => Ok(value),
        other => Err(format!("relayers() {name} has unexpected token {other:?}")),
    };
    let uint = |token: Token, name: &str| match token {
        Token::Uint(value) => Ok(value),
        other => Err(format!("relayers() {name} has unexpected token {other:?}")),
    };
    let boolean = |token: Token, name: &str| match token {
        Token::Bool(value) => Ok(value),
        other => Err(format!("relayers() {name} has unexpected token {other:?}")),
    };

    let url = string(next("url")?, "url")?;
    let ingress_url = string(next("ingressUrl")?, "ingressUrl")?;
    let metadata_url = string(next("metadataUrl")?, "metadataUrl")?;
    let staked_amount = uint(next("stakedAmount")?, "stakedAmount")?;
    let unstake_time = uint(next("unstakeTime")?, "unstakeTime")?;
    let is_registered = boolean(next("isRegistered")?, "isRegistered")?;

    let (status, frozen) = match layout {
        ProfileLayout::Legacy7 => (None, false),
        ProfileLayout::Current9 => {
            let status = uint(next("status")?, "status")?;
            let status = u8::try_from(status.as_u64())
                .ok()
                .filter(|_| status <= U256::from(u8::MAX))
                .ok_or_else(|| format!("relayers() status {status} is out of range"))?;
            (Some(status), boolean(next("frozen")?, "frozen")?)
        }
    };

    Ok(RelayerProfile {
        sphinx_key,
        url,
        ingress_url,
        metadata_url,
        staked_amount,
        unstake_time,
        is_registered,
        status,
        frozen,
        layout,
    })
}

impl RelayerProfile {
    /// A registered member of the topology set (frozen or not). On the current
    /// layout the status must also be non-`None`.
    pub fn is_member(&self) -> bool {
        self.is_registered && self.status != Some(STATUS_NONE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn common_tokens(registered: bool) -> Vec<Token> {
        vec![
            Token::FixedBytes(vec![0xab; 32]),
            Token::String("/ip4/3.236.170.102/tcp/15000/p2p/12D3KooWQreA".to_string()),
            Token::String("https://nox-1.hisoka.io".to_string()),
            Token::String(String::new()),
            Token::Uint(U256::from(7)),
            Token::Uint(U256::zero()),
            Token::Bool(registered),
        ]
    }

    #[test]
    fn decodes_the_legacy_seven_field_layout() {
        let data = abi::encode(&common_tokens(true));
        let profile = decode_relayer_profile(&data).unwrap();
        assert_eq!(profile.layout, ProfileLayout::Legacy7);
        assert_eq!(profile.sphinx_key, [0xab; 32]);
        assert_eq!(profile.ingress_url, "https://nox-1.hisoka.io");
        assert_eq!(profile.staked_amount, U256::from(7));
        assert!(profile.is_registered);
        assert_eq!(profile.status, None);
        assert!(!profile.frozen);
        assert!(profile.is_member());
    }

    #[test]
    fn decodes_the_nine_field_layout_with_status_and_frozen() {
        let mut tokens = common_tokens(true);
        tokens.push(Token::Uint(U256::from(2)));
        tokens.push(Token::Bool(true));
        let profile = decode_relayer_profile(&abi::encode(&tokens)).unwrap();
        assert_eq!(profile.layout, ProfileLayout::Current9);
        assert_eq!(profile.status, Some(2));
        assert!(profile.frozen);
        assert_eq!(profile.url, "/ip4/3.236.170.102/tcp/15000/p2p/12D3KooWQreA");
        assert!(profile.is_member());
    }

    #[test]
    fn nine_field_data_is_never_misread_as_legacy() {
        let mut tokens = common_tokens(true);
        tokens.push(Token::Uint(U256::from(1)));
        tokens.push(Token::Bool(true));
        let data = abi::encode(&tokens);
        // A naive 7-field decode succeeds and loses `frozen`; layout detection must not.
        assert!(abi::decode(&param_types(ProfileLayout::Legacy7), &data).is_ok());
        assert!(decode_relayer_profile(&data).unwrap().frozen);
    }

    #[test]
    fn unregistered_zero_profiles_decode_as_non_members() {
        let zero_legacy = abi::encode(&[
            Token::FixedBytes(vec![0; 32]),
            Token::String(String::new()),
            Token::String(String::new()),
            Token::String(String::new()),
            Token::Uint(U256::zero()),
            Token::Uint(U256::zero()),
            Token::Bool(false),
        ]);
        assert!(!decode_relayer_profile(&zero_legacy).unwrap().is_member());

        let mut current = common_tokens(false);
        current.push(Token::Uint(U256::zero()));
        current.push(Token::Bool(false));
        assert!(!decode_relayer_profile(&abi::encode(&current))
            .unwrap()
            .is_member());
    }

    #[test]
    fn rejects_truncated_and_unknown_layouts() {
        assert!(decode_relayer_profile(&[0_u8; 16]).is_err());
        let mut data = abi::encode(&common_tokens(true));
        data[WORD..2 * WORD].copy_from_slice(&[0_u8; 32]);
        assert!(decode_relayer_profile(&data).is_err());
    }

    #[test]
    fn calldata_uses_the_relayers_selector() {
        let data = relayers_calldata(Address::from_low_u64_be(1));
        assert_eq!(
            &data[..4],
            &[0x53, 0x00, 0xf8, 0x41][..],
            "selector {:02x?}",
            &data[..4]
        );
        assert_eq!(data.len(), 4 + 32);
    }
}
