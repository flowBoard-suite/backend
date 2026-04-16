package pl.flow.board.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import pl.flow.board.backend.interfaces.BoardRepository;
import pl.flow.board.backend.model.Board;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class BoardService {

    private final BoardRepository boardRepository;

    public Mono<Board> saveBoard(Board board) {
        return boardRepository.save(board);
    }

    public Flux<Board> getAllBoards() {
        return boardRepository.findAll();
    }

    public Flux<Board> getBoardsBySerialNumber(String serialNumber) {
        return boardRepository.findBySerialNumber(serialNumber);
    }

    public Mono<Board> getBoardById(String boardId) {
        return boardRepository.findById(boardId);
    }
}