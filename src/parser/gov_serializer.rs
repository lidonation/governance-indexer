use pallas::codec::utils::Nullable;
use pallas::ledger::primitives::conway::*;
use serde_json::{Value, json};

pub fn serialize_gov_action(action: &GovAction) -> (String, Value) {
    match action {
        GovAction::ParameterChange(prev_action, protocol_params, policy) => {
            let action_type = "ParameterChange".to_string();
            let prev = match prev_action {
                Nullable::Some(a) => Some(format!(
                    "{}#{}",
                    hex::encode(a.transaction_id),
                    a.action_index
                )),
                _ => None,
            };
            let pol = match policy {
                Nullable::Some(h) => Some(hex::encode(h)),
                _ => None,
            };
            let details = json!({
                "type": "ParameterChange",
                "previous_action": prev,
                "policy_hash": pol,
                "parameters": format!("{:?}", protocol_params)
            });
            (action_type, details)
        }

        GovAction::HardForkInitiation(prev_action, protocol_version) => {
            let action_type = "HardForkInitiation".to_string();
            let prev = match prev_action {
                Nullable::Some(a) => Some(format!(
                    "{}#{}",
                    hex::encode(a.transaction_id),
                    a.action_index
                )),
                _ => None,
            };
            let details = json!({
                "type": "HardForkInitiation",
                "previous_action": prev,
                "protocol_version": {
                    "major": protocol_version.0,
                    "minor": protocol_version.1
                }
            });
            (action_type, details)
        }

        GovAction::TreasuryWithdrawals(withdrawals, policy) => {
            let action_type = "TreasuryWithdrawals".to_string();
            let withdrawal_list: Vec<Value> = withdrawals
                .iter()
                .map(|(cred, amount)| {
                    json!({
                        "recipient": format!("{:?}", cred),
                        "amount_lovelace": amount
                    })
                })
                .collect();

            let pol = match policy {
                Nullable::Some(h) => Some(hex::encode(h)),
                _ => None,
            };

            let details = json!({
                "type": "TreasuryWithdrawals",
                "withdrawals": withdrawal_list,
                "policy_hash": pol
            });
            (action_type, details)
        }

        GovAction::NoConfidence(prev_action) => {
            let action_type = "NoConfidence".to_string();
            let prev = match prev_action {
                Nullable::Some(a) => Some(format!(
                    "{}#{}",
                    hex::encode(a.transaction_id),
                    a.action_index
                )),
                _ => None,
            };
            let details = json!({
                "type": "NoConfidence",
                "previous_action": prev
            });
            (action_type, details)
        }

        GovAction::UpdateCommittee(prev_action, remove_members, add_members, quorum) => {
            let action_type = "UpdateCommittee".to_string();

            let prev = match prev_action {
                Nullable::Some(a) => Some(format!(
                    "{}#{}",
                    hex::encode(a.transaction_id),
                    a.action_index
                )),
                _ => None,
            };

            let removed: Vec<String> = remove_members
                .iter()
                .map(|cred| format_committee_credential(cred))
                .collect();

            let added: Vec<Value> = add_members
                .iter()
                .map(|(cred, epoch)| {
                    json!({
                        "credential": format_committee_credential(cred),
                        "expires_epoch": epoch
                    })
                })
                .collect();

            let details = json!({
                "type": "UpdateCommittee",
                "previous_action": prev,
                "removed_members": removed,
                "added_members": added,
                "quorum": {
                    "numerator": quorum.numerator,
                    "denominator": quorum.denominator
                }
            });
            (action_type, details)
        }

        GovAction::NewConstitution(prev_action, constitution) => {
            let action_type = "NewConstitution".to_string();
            let prev = match prev_action {
                Nullable::Some(a) => Some(format!(
                    "{}#{}",
                    hex::encode(a.transaction_id),
                    a.action_index
                )),
                _ => None,
            };
            let guard = match &constitution.guardrail_script {
                Nullable::Some(h) => Some(hex::encode(h)),
                _ => None,
            };
            let details = json!({
                "type": "NewConstitution",
                "previous_action": prev,
                "constitution": {
                    "anchor_url": &constitution.anchor.url,
                    "anchor_hash": hex::encode(constitution.anchor.content_hash),
                    "guardrail_script": guard
                }
            });
            (action_type, details)
        }

        GovAction::Information => {
            let action_type = "Information".to_string();
            let details = json!({
                "type": "Information"
            });
            (action_type, details)
        }
    }
}

fn format_committee_credential(cred: &StakeCredential) -> String {
    match cred {
        StakeCredential::AddrKeyhash(hash) => format!("key:{}", hex::encode(hash)),
        StakeCredential::ScriptHash(hash) => format!("script:{}", hex::encode(hash)),
    }
}
