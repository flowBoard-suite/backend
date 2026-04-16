package pl.flow.board.backend.utils;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.common.util.concurrent.MoreExecutors;

import reactor.core.publisher.Mono;

public class FirestoreUtils {

    public static <T> Mono<T> toMono(ApiFuture<T> future) {
        return Mono.create(sink -> {
            ApiFutures.addCallback(future, new ApiFutureCallback<T>() {
                @Override
                public void onSuccess(T result) {
                    sink.success(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

}
