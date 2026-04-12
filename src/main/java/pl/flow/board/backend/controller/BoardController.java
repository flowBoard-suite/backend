package pl.flow.board.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import pl.flow.board.backend.model.Board;
import pl.flow.board.backend.service.BoardService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/boards")
@RequiredArgsConstructor
public class BoardController {

    private final BoardService boardService;

    @GetMapping
    public Flux<Board> getAllBoards() {
        return boardService.getAllBoards();
    }

    @GetMapping("/{boardId}")
    public Mono<Board> getBoardById(@PathVariable String boardId) {
        return boardService.getBoardById(boardId);
    }
}
