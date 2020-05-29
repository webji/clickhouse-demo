package org.example.clickhousedemo.cluster.query;

public enum JobPriority {
    HIGH("High", 0),
    MEDIUM("Medium", 1),
    LOW("Low", 2);


    private String name;
    private Integer value;

    JobPriority(String name, Integer value) {
        this.name = name;
        this.value = value;
    }

    public static JobPriority fromName(String name) {
        for (JobPriority priority : JobPriority.values()) {
            if (priority.name.equalsIgnoreCase(name)) {
                return priority;
            }
        }
        return JobPriority.MEDIUM;
    }

}
