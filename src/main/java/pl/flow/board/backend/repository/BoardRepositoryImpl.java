package pl.flow.board.backend.repository;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import pl.flow.board.backend.exception.custom.ResourceNotFoundException;
import pl.flow.board.backend.model.Board;
import pl.flow.board.backend.utils.FirestoreUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Repository
@RequiredArgsConstructor
public class BoardRepositoryImpl implements BoardRepository {

    private final Firestore firestore;

    @Override
    public Mono<Board> save(Board board) {
        if (board.getId() == null || board.getId().isBlank()) {
            return Mono.create(sink -> {
                ApiFuture<DocumentReference> future = firestore.collection("boards").add(board);
                ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
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

        return Mono.create(sink -> {
            ApiFuture<com.google.cloud.firestore.WriteResult> future = firestore.collection("boards")
                    .document(board.getId()).set(board);
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(com.google.cloud.firestore.WriteResult result) {
                    sink.success(board);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Flux<Board> findAll() {
        return Flux.create(sink -> {
            ApiFuture<QuerySnapshot> future = firestore.collection("boards").get();
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(QuerySnapshot result) {
                    result.getDocuments().forEach(doc -> sink.next(doc.toObject(Board.class)));
                    sink.complete();
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Flux<Board> findBySerialNumber(String serialNumber) {
        return Flux.create(sink -> {
            ApiFuture<QuerySnapshot> future = firestore.collection("boards")
                    .whereEqualTo("serialNumber", serialNumber)
                    .get();
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(QuerySnapshot result) {
                    result.getDocuments().forEach(doc -> sink.next(doc.toObject(Board.class)));
                    sink.complete();
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Mono<Board> findById(String id) {
        ApiFuture<DocumentSnapshot> future = firestore.collection("boards").document(id).get();

        return FirestoreUtils.toMono(future).flatMap(snapshot -> {
            if (!snapshot.exists()) {
                return Mono.error(new ResourceNotFoundException("Board not found"));
            }

            return Mono.justOrEmpty(snapshot.toObject(Board.class));

        })
                .log("BoardRepositoryImpl.findById")
                .subscribeOn(Schedulers.boundedElastic());
    }
}
