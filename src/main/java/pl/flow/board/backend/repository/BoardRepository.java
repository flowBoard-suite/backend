package pl.flow.board.backend.repository;

import pl.flow.board.backend.model.Board;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface BoardRepository {
    Mono<Board> save(Board board);
    Flux<Board> findAll();
    Flux<Board> findBySerialNumber(String serialNumber);
    Mono<Board> findById(String id);
}
