package io.github.fullstacknick.outboxer.contract;

import java.util.List;

public final class ContractViolationException extends RuntimeException {

    private final List<String> violations;

    public ContractViolationException(List<String> violations) {
        super(String.join("; ", violations));
        this.violations = List.copyOf(violations);
    }

    public List<String> violations() {
        return violations;
    }
}
