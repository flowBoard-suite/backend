package pl.flow.board.backend.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import lombok.extern.slf4j.Slf4j;
import pl.flow.board.backend.exception.custom.ResourceNotFoundException;
import reactor.core.publisher.Mono;

@ControllerAdvice
@Slf4j
public class CrudExceptionHandler {

    @ExceptionHandler(ResourceNotFoundException.class)
    public Mono<ResponseEntity<ErrorResponse>> handleResourceNotFoundException(ResourceNotFoundException ex) {
        final var status = HttpStatus.NOT_FOUND;
        log.error("Resource not found: {}", ex.getMessage());
        return Mono.just(ResponseEntity.status(status).body(new ErrorResponse(ex.getMessage(), status)));
    }
}
