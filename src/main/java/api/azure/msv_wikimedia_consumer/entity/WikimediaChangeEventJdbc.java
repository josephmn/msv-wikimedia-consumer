package api.azure.msv_wikimedia_consumer.entity;

import lombok.Data;

@Data
public class WikimediaChangeEventJdbc {

    private String $schema;
    private Meta meta;
    private long id;
    private String type;
    private int namespace;
    private String title;
    private String titleUrl;
    private String comment;
    private long timestamp;
    private String user;
    private boolean bot;
    private String notifyUrl;
    private boolean minor;
    private boolean patrolled;
    private Length length;
    private Revision revision;
    private String serverUrl;
    private String serverName;
    private String serverScriptPath;
    private String wiki;

    @Data
    public static class Meta {
        private String uri;
        private String requestId;
        private String id;
        private String dt;
        private String domain;
        private String stream;
        private String topic;
        private int partition;
        private long offset;
    }

    @Data
    public static class Length {
        private int old;
        private int $new;
    }

    @Data
    public static class Revision {
        private long old;
        private long $new;
    }
}
