package pl.flow.board.backend.controller;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.WriteResult;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final Firestore firestore;

    @GetMapping("/test-add")
    public Mono<String> testAdd() {
        Map<String, Object> data = Map.of("status", "dziala");
        ApiFuture<WriteResult> apiFuture = firestore.collection("testy").document("start").set(data);

        return Mono.create(sink -> {
            ApiFutures.addCallback(apiFuture, new ApiFutureCallback<WriteResult>() {
                @Override
                public void onSuccess(WriteResult result) {
                    sink.success("Zapisano: " + result.getUpdateTime());
                }

                @Override
                public void onFailure(Throwable t) {
                    sink.error(t);
                }
            }, MoreExecutors.directExecutor());
        });
    }
}