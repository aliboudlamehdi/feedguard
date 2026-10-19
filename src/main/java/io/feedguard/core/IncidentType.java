package io.feedguard.core;

public enum IncidentType {
    /** Missing or out-of-order update ids: the local book can no longer be trusted. */
    SEQUENCE_GAP,
    /** Venue-provided book checksum does not match the locally maintained book. */
    CHECKSUM_MISMATCH,
    /** Venue sends a placeholder checksum (0): integrity relies on sequence ids only. Reported once. */
    CHECKSUM_UNAVAILABLE,
    /** Best bid >= best ask on a synced book. */
    CROSSED_BOOK,
    BOOK_UNCROSSED,
    /** No message on a stream for longer than its threshold. */
    STALE_FEED,
    FEED_RECOVERED,
    DISCONNECT,
    RECONNECTED,
    /** Message shape changed (fields added or removed) compared to the first message seen. */
    SCHEMA_DRIFT,
    /** Book (re)built from a snapshot. */
    BOOK_SYNCED,
    SNAPSHOT_FAILED,
    PARSE_ERROR,
    /** Error message pushed by the venue itself (e.g. subscription rejected). */
    VENUE_ERROR
}
