package pl.flow.board.backend.interfaces;

import pl.flow.board.backend.model.User;
import reactor.core.publisher.Mono;

public interface UserRepository {
    Mono<User> save(User user);

    Mono<User> findById(String id);
}
