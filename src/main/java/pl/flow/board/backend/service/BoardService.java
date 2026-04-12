package pl.flow.board.backend.service;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pl.flow.board.backend.model.Board;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class BoardService {

    private final Firestore firestore;

    public Mono<Board> saveBoard(Board board) {
        return Mono.create(sink -> {
            ApiFuture<DocumentReference> future = firestore.collection("boards").add(board);

            ApiFutures.addCallback(future, new ApiFutureCallback<>() {
                @Override
                public void onSuccess(DocumentReference result) {
                    board.setId(result.getId());
                    sink.success(board);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    public Flux<Board> getAllBoards() {
        return Flux.create(sink -> {
            ApiFuture<QuerySnapshot> future = firestore.collection("boards").get();

            ApiFutures.addCallback(future, new ApiFutureCallback<>() {
                @Override
                public void onSuccess(QuerySnapshot result) {
                    result.getDocuments().forEach(doc -> {
                        Board board = doc.toObject(Board.class);
                        sink.next(board);
                    });
                    sink.complete();
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    public Mono<Board> getBoardById(String boardId) {
        return Mono.create(sink -> {
            ApiFuture<DocumentSnapshot> future = firestore.collection("boards")
                    .document(boardId).get();

            ApiFutures.addCallback(future, new ApiFutureCallback<>() {
                @Override
                public void onSuccess(DocumentSnapshot result) {
                    if (result.exists()) {
                        sink.success(result.toObject(Board.class));
                    } else {
                        sink.error(new RuntimeException("Board not found with id: " + boardId));
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }
}