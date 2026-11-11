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

use alloy::primitives::{keccak256, Address, Bytes, FixedBytes, U256};
use alloy::sol_types::SolValue;

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
    data.extend_from_slice(address.into_word().as_slice());
    data.into()
}

/// `(sphinxKey, url, ingressUrl, metadataUrl, stakedAmount, unstakeTime, isRegistered)`.
type LegacyFields = (FixedBytes<32>, String, String, String, U256, U256, bool);
/// The legacy fields plus `(status, frozen)`. `status` is read as a full word
/// so a value above `u8::MAX` is reported instead of truncated.
type CurrentFields = (
    FixedBytes<32>,
    String,
    String,
    String,
    U256,
    U256,
    bool,
    U256,
    bool,
);

fn detect_layout(data: &[u8]) -> Result<ProfileLayout, String> {
    if data.len() < 2 * WORD {
        return Err(format!(
            "relayers() returned {} bytes, too short for a profile",
            data.len()
        ));
    }
    let offset = U256::from_be_slice(&data[WORD..2 * WORD]);
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
    let decode_failed =
        |e: alloy::sol_types::Error| format!("relayers() {layout:?} decode failed: {e}");
    let (fields, status, frozen) = match layout {
        ProfileLayout::Legacy7 => {
            let fields = LegacyFields::abi_decode_params(data).map_err(decode_failed)?;
            (fields, None, false)
        }
        ProfileLayout::Current9 => {
            let (key, url, ingress, metadata, stake, unstake, registered, status, frozen) =
                CurrentFields::abi_decode_params(data).map_err(decode_failed)?;
            let status = u8::try_from(status)
                .map_err(|_| format!("relayers() status {status} is out of range"))?;
            (
                (key, url, ingress, metadata, stake, unstake, registered),
                Some(status),
                frozen,
            )
        }
    };
    let (sphinx_key, url, ingress_url, metadata_url, staked_amount, unstake_time, is_registered) =
        fields;
    let sphinx_key = sphinx_key.0;

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

    fn legacy(registered: bool) -> LegacyFields {
        (
            FixedBytes::repeat_byte(0xab),
            "/ip4/3.236.170.102/tcp/15000/p2p/12D3KooWQreA".to_string(),
            "https://nox-1.hisoka.io".to_string(),
            String::new(),
            U256::from(7),
            U256::ZERO,
            registered,
        )
    }

    fn current(registered: bool, status: u64, frozen: bool) -> CurrentFields {
        let (key, url, ingress, metadata, stake, unstake, registered) = legacy(registered);
        (
            key,
            url,
            ingress,
            metadata,
            stake,
            unstake,
            registered,
            U256::from(status),
            frozen,
        )
    }

    #[test]
    fn decodes_the_legacy_seven_field_layout() {
        let data = legacy(true).abi_encode_params();
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
        let profile = decode_relayer_profile(&current(true, 2, true).abi_encode_params()).unwrap();
        assert_eq!(profile.layout, ProfileLayout::Current9);
        assert_eq!(profile.status, Some(2));
        assert!(profile.frozen);
        assert_eq!(profile.url, "/ip4/3.236.170.102/tcp/15000/p2p/12D3KooWQreA");
        assert!(profile.is_member());
    }

    #[test]
    fn nine_field_data_is_never_misread_as_legacy() {
        let data = current(true, 1, true).abi_encode_params();
        // A naive 7-field decode succeeds and loses `frozen`; layout detection must not.
        assert!(LegacyFields::abi_decode_params(&data).is_ok());
        assert!(decode_relayer_profile(&data).unwrap().frozen);
    }

    #[test]
    fn status_above_u8_is_rejected() {
        let data = current(true, 256, false).abi_encode_params();
        let error = decode_relayer_profile(&data).unwrap_err();
        assert!(error.contains("status 256 is out of range"), "{error}");
    }

    #[test]
    fn unregistered_zero_profiles_decode_as_non_members() {
        let zero_legacy: LegacyFields = (
            FixedBytes::ZERO,
            String::new(),
            String::new(),
            String::new(),
            U256::ZERO,
            U256::ZERO,
            false,
        );
        assert!(!decode_relayer_profile(&zero_legacy.abi_encode_params())
            .unwrap()
            .is_member());
        assert!(
            !decode_relayer_profile(&current(false, 0, false).abi_encode_params())
                .unwrap()
                .is_member()
        );
    }

    #[test]
    fn rejects_truncated_and_unknown_layouts() {
        assert!(decode_relayer_profile(&[0_u8; 16]).is_err());
        let mut data = legacy(true).abi_encode_params();
        data[WORD..2 * WORD].copy_from_slice(&[0_u8; 32]);
        assert!(decode_relayer_profile(&data).is_err());
    }

    #[test]
    fn calldata_uses_the_relayers_selector() {
        let data = relayers_calldata(Address::with_last_byte(1));
        assert_eq!(
            &data[..4],
            &[0x53, 0x00, 0xf8, 0x41][..],
            "selector {:02x?}",
            &data[..4]
        );
        assert_eq!(data.len(), 4 + 32);
    }
}
