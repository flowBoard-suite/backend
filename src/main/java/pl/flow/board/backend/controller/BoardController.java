package pl.flow.board.backend.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
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

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Board> createBoard(@RequestBody Board board) {
        return boardService.saveBoard(board);
    }

    // Zmodyfikowany endpoint GET
    @GetMapping
    public Flux<Board> getBoards(@RequestParam(name = "serialNumber", required = false) String serialNumber) {
        if (serialNumber != null && !serialNumber.isBlank()) {
            return boardService.getBoardsBySerialNumber(serialNumber);
        }
        return boardService.getAllBoards();
    }

    @GetMapping("/{boardId}")
    public Mono<Board> getBoardById(@PathVariable String boardId) {
        return boardService.getBoardById(boardId);
    }
}