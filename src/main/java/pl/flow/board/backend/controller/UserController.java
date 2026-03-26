package pl.flow.board.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pl.flow.board.backend.model.Board;
import pl.flow.board.backend.model.User;
import pl.flow.board.backend.service.UserService;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/{userId}/board")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<User> createBoard(@PathVariable String userId, @RequestBody Board board) {
        return userService.assignBoardToUser(userId, board);
    }
}
