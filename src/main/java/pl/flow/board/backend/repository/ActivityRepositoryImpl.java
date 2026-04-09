package pl.flow.board.backend.repository;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import pl.flow.board.backend.model.Activity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class ActivityRepositoryImpl implements ActivityRepository {

    private final Firestore firestore;

    @Override
    public Mono<Activity> save(Activity activity) {
        if (activity.getId() == null || activity.getId().isBlank()) {
            return Mono.create(sink -> {
                ApiFuture<DocumentReference> future = firestore.collection("activities").add(activity);
                ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                    @Override
                    public void onSuccess(DocumentReference result) {
                        activity.setId(result.getId());
                        sink.success(activity);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        sink.error(t);
                    }
                }, MoreExecutors.directExecutor());
            });
        }

        return Mono.create(sink -> {
            ApiFuture<com.google.cloud.firestore.WriteResult> future = firestore.collection("activities").document(activity.getId()).set(activity);
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(com.google.cloud.firestore.WriteResult result) {
                    sink.success(activity);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Flux<Activity> findAllByBoardId(String boardId) {
        return Flux.create(sink -> {
            ApiFuture<QuerySnapshot> future = firestore.collection("activities")
                    .whereEqualTo("boardId", boardId)
                    .get();
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(QuerySnapshot result) {
                    result.getDocuments().forEach(doc -> sink.next(doc.toObject(Activity.class)));
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
    public Mono<Activity> findById(String id) {
        return Mono.create(sink -> {
            ApiFuture<com.google.cloud.firestore.DocumentSnapshot> future = firestore.collection("activities").document(id).get();
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(com.google.cloud.firestore.DocumentSnapshot result) {
                    if (result.exists()) {
                        sink.success(result.toObject(Activity.class));
                    } else {
                        sink.success();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    @Override
    public Mono<Void> deleteById(String id) {
        return Mono.create(sink -> {
            ApiFuture<com.google.cloud.firestore.WriteResult> future = firestore.collection("activities").document(id).delete();
            ApiFutures.addCallback(future, new com.google.api.core.ApiFutureCallback<>() {
                @Override
                public void onSuccess(com.google.cloud.firestore.WriteResult result) {
                    sink.success();
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }
}
