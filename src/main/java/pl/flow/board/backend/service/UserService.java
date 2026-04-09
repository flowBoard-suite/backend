package pl.flow.board.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pl.flow.board.backend.model.Board;
import pl.flow.board.backend.model.User;
import pl.flow.board.backend.repository.UserRepository;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final BoardService boardService;

    public Mono<User> getUserById(String userId) {
        return userRepository.findById(userId)
                .switchIfEmpty(Mono.error(new RuntimeException("Użytkownik o ID " + userId + " nie istnieje")));
    }

    public Mono<User> assignBoardToUser(String userId, Board board) {
        return boardService.saveBoard(board)
                .flatMap(savedBoard -> userRepository.findById(userId)
                        .switchIfEmpty(Mono.error(new RuntimeException("Użytkownik o ID " + userId + " nie istnieje")))
                        .flatMap(user -> {
                            user.setBoardId(savedBoard.getId());
                            return userRepository.save(user);
                        })
                );
    }
}
