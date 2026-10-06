package com.simplekafka.tiered;

/** Identifies one sealed partition segment in remote storage. */
public record RemoteSegmentId(String topic, int partition, long baseOffset) {

    /** Object key stem, e.g. {@code orders/3/00000000000000001024}; files append .log.zst / .index.zst. */
    public String keyPrefix() {
        return topic + "/" + partition + "/" + String.format("%020d", baseOffset);
    }

    public String logKey() {
        return keyPrefix() + ".log.zst";
    }

    public String indexKey() {
        return keyPrefix() + ".index.zst";
    }
}
