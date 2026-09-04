package io.github.fullstacknick.outboxer.central;

import io.github.fullstacknick.outboxer.contract.ContractViolationException;
import io.github.fullstacknick.outboxer.contract.Topics;
import java.util.List;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/machines/{machineId}")
class CycleController {

    private final CycleQueryRepository repository;

    CycleController(CycleQueryRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/cycles/latest")
    Mono<ResponseEntity<CycleResponse>> latest(
            @PathVariable String machineId, @RequestHeader("X-Tenant-Id") String tenantId) {
        validateScope(tenantId, machineId);
        return repository.latest(tenantId, machineId)
                .map(value -> ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Machine cycle not found")));
    }

    @GetMapping("/cycles")
    Mono<ResponseEntity<List<CycleResponse>>> recent(
            @PathVariable String machineId,
            @RequestHeader("X-Tenant-Id") String tenantId,
            @RequestParam(defaultValue = "50") int limit) {
        validateScope(tenantId, machineId);
        if (limit < 1 || limit > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and 100");
        }
        return repository.recent(tenantId, machineId, limit)
                .collectList()
                .map(value -> ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value));
    }

    @GetMapping("/data-quality")
    Mono<ResponseEntity<DataQualityResponse>> quality(
            @PathVariable String machineId, @RequestHeader("X-Tenant-Id") String tenantId) {
        validateScope(tenantId, machineId);
        return repository.quality(tenantId, machineId)
                .map(value -> ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(value))
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Machine quality data not found")));
    }

    private static void validateScope(String tenantId, String machineId) {
        try {
            Topics.requireIdentifier(tenantId, "tenantId");
            Topics.requireIdentifier(machineId, "machineId");
        } catch (ContractViolationException exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, exception.getMessage());
        }
    }
}
