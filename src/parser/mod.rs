use bech32::{self, ToBase32, Variant};
use pallas::ledger::primitives::conway::{Certificate, DRep, StakeCredential, Voter};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::ledger::traverse::MultiEraCert;
use serde::Serialize;

mod gov_serializer;

#[derive(Debug, Clone, Serialize)]
pub enum GovEvent {
    Registration {
        tx_hash: String,
        tx_index: u32,
        drep_id: String,
        deposit: u64,
        anchor_url: Option<String>,
        anchor_hash: Option<String>,
    },
    Retirement {
        tx_hash: String,
        tx_index: u32,
        drep_id: String,
        deposit: u64,
    },
    Vote {
        tx_hash: String,
        tx_index: u32,
        voter_id: String,
        proposal_tx_hash: String,
        proposal_index: u32,
        vote: String,
        anchor_url: Option<String>,
        anchor_hash: Option<String>,
    },
    Delegation {
        tx_hash: String,
        tx_index: u32,
        stake_credential: String,
        drep_id: String,
        drep_has_script: bool,
    },
    Proposal {
        tx_hash: String,
        tx_index: u32,
        index: u32,
        action_type: String,
        deposit: u64,
        anchor_url: Option<String>,
        anchor_hash: Option<String>,
        reward_account: Option<String>,
        details: serde_json::Value,
    },
}

fn hash_to_bech32(hrp: &str, hash: &[u8]) -> String {
    match bech32::encode(hrp, hash.to_base32(), Variant::Bech32) {
        Ok(s) => s,
        Err(_) => hex::encode(hash),
    }
}

fn format_drep(drep: &DRep) -> String {
    match drep {
        DRep::Key(hash) => hash_to_bech32("drep", hash.as_slice()),
        DRep::Script(hash) => hash_to_bech32("drep", hash.as_slice()),
        DRep::Abstain => "drep-always-abstain".to_string(),
        DRep::NoConfidence => "drep-always-no-confidence".to_string(),
    }
}

fn format_stake(cred: &StakeCredential) -> String {
    match cred {
        StakeCredential::AddrKeyhash(hash) => hash_to_bech32("stake", hash.as_slice()),
        StakeCredential::ScriptHash(hash) => hash_to_bech32("stake", hash.as_slice()),
    }
}

fn format_voter(voter: &Voter) -> String {
    match voter {
        Voter::DRepKey(hash) => hash_to_bech32("drep", hash.as_slice()),
        Voter::DRepScript(hash) => hash_to_bech32("drep", hash.as_slice()),
        Voter::ConstitutionalCommitteeKey(hash) => hash_to_bech32("cc_hot", hash.as_slice()),
        Voter::ConstitutionalCommitteeScript(hash) => hash_to_bech32("cc_hot", hash.as_slice()),
        Voter::StakePoolKey(hash) => hash_to_bech32("pool", hash.as_slice()),
    }
}

pub fn extract_governance(block_bytes: &[u8]) -> Vec<GovEvent> {
    let mut events = Vec::new();

    let block = match MultiEraBlock::decode(block_bytes) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("block decode failed in extract_governance: {}", e);
            return events;
        }
    };

    for tx in block.txs() {
        let tx_hash = tx.hash().to_string();
        let mut tx_index = 0u32;

        // 1. Certificates
        for cert in tx.certs() {
            if let MultiEraCert::Conway(c) = cert {
                match **c {
                    // RegDRepCert(StakeCredential, Coin, Nullable<Anchor>)
                    Certificate::RegDRepCert(ref cred, coin, ref anchor) => {
                        let (anchor_url, anchor_hash) = match anchor {
                            pallas::codec::utils::Nullable::Some(a) => {
                                (Some(a.url.clone()), Some(hex::encode(a.content_hash)))
                            }
                            _ => (None, None),
                        };

                        let drep_id = match cred {
                            StakeCredential::AddrKeyhash(h) => hash_to_bech32("drep", h.as_slice()),
                            StakeCredential::ScriptHash(h) => hash_to_bech32("drep", h.as_slice()),
                        };

                        events.push(GovEvent::Registration {
                            tx_hash: tx_hash.clone(),
                            tx_index,
                            drep_id,
                            deposit: coin,
                            anchor_url,
                            anchor_hash,
                        });
                        tx_index += 1;
                    }
                    // UnRegDRepCert(StakeCredential, Coin)
                    Certificate::UnRegDRepCert(ref cred, coin) => {
                        let drep_id = match cred {
                            StakeCredential::AddrKeyhash(h) => hash_to_bech32("drep", h.as_slice()),
                            StakeCredential::ScriptHash(h) => hash_to_bech32("drep", h.as_slice()),
                        };
                        events.push(GovEvent::Retirement {
                            tx_hash: tx_hash.clone(),
                            tx_index,
                            drep_id,
                            deposit: coin,
                        });
                        tx_index += 1;
                    }
                    // VoteDeleg(StakeCredential, DRep)
                    Certificate::VoteDeleg(ref stake, ref drep) => {
                        let drep_has_script = match drep {
                            DRep::Script(_) => true,
                            _ => false,
                        };

                        events.push(GovEvent::Delegation {
                            tx_hash: tx_hash.clone(),
                            tx_index,
                            stake_credential: format_stake(stake),
                            drep_id: format_drep(drep),
                            drep_has_script,
                        });
                        tx_index += 1;
                    }
                    _ => {}
                }
            }
        }

        // 2. Conway Specifics (Votes & Proposals)
        if let pallas::ledger::traverse::MultiEraTx::Conway(mtx) = tx {
            // Votes
            if let Some(voting_procedures) = &mtx.transaction_body.voting_procedures {
                for (voter, votes) in voting_procedures.iter() {
                    for (action_id, procedure) in votes.iter() {
                        let (anchor_url, anchor_hash) = match &procedure.anchor {
                            pallas::codec::utils::Nullable::Some(a) => {
                                (Some(a.url.clone()), Some(hex::encode(a.content_hash)))
                            }
                            _ => (None, None),
                        };

                        events.push(GovEvent::Vote {
                            tx_hash: tx_hash.clone(),
                            tx_index,
                            voter_id: format_voter(voter),
                            proposal_tx_hash: hex::encode(action_id.transaction_id),
                            proposal_index: action_id.action_index,
                            vote: format!("{:?}", procedure.vote),
                            anchor_url,
                            anchor_hash,
                        });
                        tx_index += 1;
                    }
                }
            }

            // Proposals
            if let Some(proposals) = &mtx.transaction_body.proposal_procedures {
                for (idx, proposal) in proposals.iter().enumerate() {
                    let raw_ra = proposal.reward_account.to_vec();
                    let ra_hrp = if raw_ra.first().copied() == Some(0xe1) { "stake_test" } else { "stake" };
                    let reward_account = Some(hash_to_bech32(ra_hrp, &raw_ra));
                    let (action_type, details) =
                        gov_serializer::serialize_gov_action(&proposal.gov_action);

                    events.push(GovEvent::Proposal {
                        tx_hash: tx_hash.clone(),
                        tx_index,
                        index: idx as u32,
                        action_type,
                        deposit: proposal.deposit,
                        anchor_url: Some(proposal.anchor.url.clone()),
                        anchor_hash: Some(hex::encode(proposal.anchor.content_hash)),
                        reward_account,
                        details,
                    });
                    tx_index += 1;
                }
            }
        }
    }

    events
}
