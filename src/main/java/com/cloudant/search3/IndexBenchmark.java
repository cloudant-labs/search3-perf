package com.cloudant.search3;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import com.cloudant.search3.grpc.Search3.DocumentField;
import com.cloudant.search3.grpc.Search3.DocumentUpdateRequest;
import com.cloudant.search3.grpc.Search3.FieldValue;
import com.cloudant.search3.grpc.Search3.Index;
import com.cloudant.search3.grpc.Search3.UpdateSeq;
import com.cloudant.search3.grpc.SearchGrpc;
import com.cloudant.search3.grpc.SearchGrpc.SearchBlockingStub;
import com.cloudant.search3.grpc.SearchGrpc.SearchStub;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

@BenchmarkMode(Mode.All)
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.MINUTES)
@OutputTimeUnit(TimeUnit.SECONDS)
public class IndexBenchmark {

    private Index index;
    private ManagedChannel channel;
    private SearchBlockingStub blockingStub;
    private SearchStub asyncStub;
    private final AtomicLong counter = new AtomicLong();

    @Setup(Level.Trial)
    public void startClient() throws Exception {
        index = Index.newBuilder().setPrefix(ByteString.copyFrom(new byte[] { 1, 2, 3 })).build();

        channel = ManagedChannelBuilder.forAddress("127.0.0.1", 8443).usePlaintext().build();
        blockingStub = SearchGrpc.newBlockingStub(channel);
        asyncStub = SearchGrpc.newStub(channel);

        counter.set(0);
        blockingStub.delete(index);
    }

    @TearDown(Level.Trial)
    public void stopClient() throws Exception {
        blockingStub.delete(index);
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Benchmark
    public void index() {
        final DocumentUpdateRequest.Builder builder = DocumentUpdateRequest.newBuilder();
        long seq = counter.incrementAndGet();
        builder.setIndex(index);
        builder.setId("doc" + seq);
        builder.setSeq(UpdateSeq.newBuilder().setSeq("seq-" + seq));
        builder.addFields(field("foo", "bar baz", true, false));
        blockingStub.updateDocument(builder.build());
    }

    private DocumentField.Builder field(
            final String name,
            final Object value,
            final boolean analyzed,
            final boolean facet) {
        return DocumentField.newBuilder().setName(name).setValue(fieldValue(value)).setAnalyzed(analyzed).setStore(true)
                .setFacet(facet);
    }

    private FieldValue.Builder fieldValue(final Object value) {
        if (value instanceof String) {
            return FieldValue.newBuilder().setString((String) value);
        }
        if (value instanceof Double) {
            return FieldValue.newBuilder().setDouble((double) value);
        }
        throw new IllegalArgumentException(value + " not recognised as field value.");
    }
}
