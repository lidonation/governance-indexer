# Feature Spec: [Feature Name]

**Status**: [Draft / Ready for Dev / In Progress / Completed]
**Target Component**: [Parser / Scanner / Monitor / DB / CLI / Infrastructure]

## 1. Intent
- **As a** [persona — indexer operator, platform consumer, on-chain participant]
- **I want to** [action]
- **So that** [value]

## 2. Functional Requirements
*Explicit, unambiguous rules. The AI implements exactly these — nothing more.*
1. Requirement 1
2. Requirement 2

## 3. Edge Cases & Error Handling
- What happens on a chain rollback past this event?
- What if the source data is missing or malformed?
- What if the DB insert conflicts?

## 4. Technical Design
*Architectural constraints, schema changes, module boundaries.*
- **Schema**: Add/change columns in `drep_timeline_event` or other tables.
- **Module**: Which `src/` module owns this logic?
- **Cardano specifics**: Relevant CIP numbers, certificate types, era constraints.

## 5. Required Test Cases (TDD)
*Write these as `#[tokio::test]` or `#[test]` in the relevant module. They must fail before implementation begins.*
- [ ] Test 1: ...
- [ ] Test 2: ...
- [ ] Test 3: ...
