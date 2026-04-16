package pl.flow.board.backend.interfaces;

import pl.flow.board.backend.model.Activity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ActivityRepository {
    Mono<Activity> save(Activity activity);

    Flux<Activity> findAllByBoardId(String boardId);

    Mono<Activity> findById(String id);

    Mono<Void> deleteById(String id);
}
