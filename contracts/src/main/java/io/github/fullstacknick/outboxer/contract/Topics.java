package io.github.fullstacknick.outboxer.contract;

import java.util.Objects;
import java.util.regex.Pattern;

public final class Topics {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z0-9._-]{1,64}");

    private Topics() {}

    public static String factoryCycles(String machineId) {
        return "factory/" + requireIdentifier(machineId, "machineId") + "/cycles";
    }

    public static String uplinkCycles(String siteId, String machineId) {
        return "uplink/" + requireIdentifier(siteId, "siteId") + "/" + requireIdentifier(machineId, "machineId")
                + "/cycles";
    }

    public static String receipts(String siteId) {
        return "receipts/" + requireIdentifier(siteId, "siteId") + "/cycles";
    }

    public static void requireFactoryTopic(String topic, MachineCycleEvent event) {
        requireTopic(topic, factoryCycles(event.machineId()));
    }

    public static void requireUplinkTopic(String topic, CycleEvent event) {
        requireTopic(topic, uplinkCycles(event.siteId(), event.machineId()));
    }

    public static String requireIdentifier(String value, String field) {
        Objects.requireNonNull(value, field);
        if (!IDENTIFIER.matcher(value).matches()) {
            throw new ContractViolationException(ListSupport.single(field + " contains unsupported characters"));
        }
        return value;
    }

    private static void requireTopic(String actual, String expected) {
        if (!expected.equals(actual)) {
            throw new ContractViolationException(ListSupport.single("topic does not match payload identity"));
        }
    }

    private static final class ListSupport {
        private static java.util.List<String> single(String value) {
            return java.util.List.of(value);
        }
    }
}
