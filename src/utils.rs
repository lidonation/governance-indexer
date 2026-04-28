use crate::config::cardano;
use chrono::{DateTime, TimeZone, Utc};

pub fn slot_to_timestamp(slot: u64) -> DateTime<Utc> {
    let slots_since_shelley = slot.saturating_sub(cardano::SHELLEY_START_SLOT);
    let seconds_offset = slots_since_shelley * cardano::SLOT_LENGTH_SECONDS;
    let timestamp = cardano::SHELLEY_START_TIME + seconds_offset as i64;
    Utc.timestamp_opt(timestamp, 0).unwrap()
}

pub fn slot_to_epoch(slot: u64) -> u64 {
    let slots_since_shelley = slot.saturating_sub(cardano::SHELLEY_START_SLOT);
    let epochs_since_shelley = slots_since_shelley / cardano::EPOCH_LENGTH_SLOTS;
    cardano::SHELLEY_START_EPOCH + epochs_since_shelley
}
