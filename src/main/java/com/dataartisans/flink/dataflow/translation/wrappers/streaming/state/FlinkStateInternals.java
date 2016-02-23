/*
 * Copyright 2015 Data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataartisans.flink.dataflow.translation.wrappers.streaming.state;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.util.state.*;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.flink.util.InstantiationUtil;
import org.joda.time.Instant;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

public class FlinkStateInternals<K> implements StateInternals<K> {

    private final K key;

    private final Coder<K> keyCoder;

    private final Coder<? extends BoundedWindow> windowCoder;

    private final Combine.KeyedCombineFn<K, ?, ?, ?> combineFn;

    private final OutputTimeFn<? super BoundedWindow> outputTimeFn;

    private Instant watermarkHoldAccessor;

    public FlinkStateInternals(K key,
                               Coder<K> keyCoder,
                               Coder<? extends BoundedWindow> windowCoder,
                               Combine.KeyedCombineFn<K, ?, ?, ?> combineFn,
                               OutputTimeFn<? super BoundedWindow> outputTimeFn) {
        this.key = key;
        this.keyCoder = keyCoder;
        this.windowCoder = windowCoder;
        this.combineFn = combineFn;
        this.outputTimeFn = outputTimeFn;
    }

    public Instant getWatermarkHold() {
        return watermarkHoldAccessor;
    }

    /**
     * This is the interface state has to implement in order for it to be fault tolerant when
     * executed by the FlinkPipelineRunner.
     */
    private interface CheckpointableIF {

        boolean shouldPersist();

        void persistState(StateCheckpointWriter checkpointBuilder) throws IOException;
    }

    protected final StateTable<K> inMemoryState = new StateTable<K>() {

        @Override
        protected StateTag.StateBinder binderForNamespace(final StateNamespace namespace) {
            return new StateTag.StateBinder<K>() {

                @Override
                public <T> ValueState<T> bindValue(StateTag<? super K, ValueState<T>> address, Coder<T> coder) {
                    return new FlinkInMemoryValue<>(encodeKey(namespace, address), coder);
                }

                @Override
                public <T> BagState<T> bindBag(StateTag<? super K, BagState<T>> address, Coder<T> elemCoder) {
                    return new FlinkInMemoryBag<>(encodeKey(namespace, address), elemCoder);
                }

                @Override
                public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT> bindCombiningValue(
                        StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                        Coder<AccumT> accumCoder, Combine.CombineFn<InputT, AccumT, OutputT> combineFn) {
                    return new FlinkInMemoryKeyedCombiningValue<>(encodeKey(namespace, address), combineFn.<K>asKeyedFn(), accumCoder);
                }

                @Override
                public <InputT, AccumT, OutputT> CombiningValueStateInternal<InputT, AccumT, OutputT> bindKeyedCombiningValue(
                        StateTag<? super K, CombiningValueStateInternal<InputT, AccumT, OutputT>> address,
                        Coder<AccumT> accumCoder, Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn) {
                    return new FlinkInMemoryKeyedCombiningValue<>(encodeKey(namespace, address), combineFn, accumCoder);
                }

                @Override
                public <W extends BoundedWindow> WatermarkStateInternal<W> bindWatermark(StateTag<? super K, WatermarkStateInternal<W>> address, OutputTimeFn<? super W> outputTimeFn) {
                    return new FlinkWatermarkStateInternalImpl<>(encodeKey(namespace, address), outputTimeFn);
                }
            };
        }
    };

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public <StateT extends State> StateT state(StateNamespace namespace, StateTag<? super K, StateT> address) {
        return inMemoryState.get(namespace, address);
    }

    public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
        checkpointBuilder.writeInt(getNoOfElements());

        for (State location : inMemoryState.values()) {
            if (!(location instanceof CheckpointableIF)) {
                throw new IllegalStateException(String.format(
                        "%s wasn't created by %s -- unable to persist it",
                        location.getClass().getSimpleName(),
                        getClass().getSimpleName()));
            }
            ((CheckpointableIF) location).persistState(checkpointBuilder);
        }
    }

    public void restoreState(StateCheckpointReader checkpointReader, ClassLoader loader)
            throws IOException, ClassNotFoundException {

        // the number of elements to read.
        int noOfElements = checkpointReader.getInt();
        for (int i = 0; i < noOfElements; i++) {
            decodeState(checkpointReader, loader);
        }
    }

    /**
     * We remove the first character which encodes the type of the stateTag ('s' for system
     * and 'u' for user). For more details check out the source of
     * {@link StateTags.StateTagBase#getId()}.
     */
    private void decodeState(StateCheckpointReader reader, ClassLoader loader)
            throws IOException, ClassNotFoundException {

        StateType stateItemType = StateType.deserialize(reader);
        ByteString stateKey = reader.getTag();

        // first decode the namespace and the tagId...
        String[] namespaceAndTag = stateKey.toStringUtf8().split("\\+");
        if (namespaceAndTag.length != 2) {
            throw new IllegalArgumentException("Invalid stateKey " + stateKey.toString() + ".");
        }
        StateNamespace namespace = StateNamespaces.fromString(namespaceAndTag[0], windowCoder);

        // ... decide if it is a system or user stateTag...
        char ownerTag = namespaceAndTag[1].charAt(0);
        if (ownerTag != 's' && ownerTag != 'u') {
            throw new RuntimeException("Invalid StateTag name.");
        }
        boolean isSystemTag = ownerTag == 's';
        String tagId = namespaceAndTag[1].substring(1);

        // ...then decode the coder (if there is one)...
        Coder<?> coder = null;
        if (!stateItemType.equals(StateType.WATERMARK)) {
            ByteString coderBytes = reader.getData();
            coder = InstantiationUtil.deserializeObject(coderBytes.toByteArray(), loader);
        }

        //... and finally, depending on the type of the state being decoded,
        // 1) create the adequate stateTag,
        // 2) create the state container,
        // 3) restore the actual content.
        switch (stateItemType) {
            case VALUE: {
                StateTag stateTag = StateTags.value(tagId, coder);
                stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
                @SuppressWarnings("unchecked")
                FlinkInMemoryValue<?> value = (FlinkInMemoryValue<?>) inMemoryState.get(namespace, stateTag);
                value.restoreState(reader);
                break;
            }
            case WATERMARK: {
                @SuppressWarnings("unchecked")
                StateTag<Object, WatermarkStateInternal<BoundedWindow>> stateTag = StateTags.watermarkStateInternal(tagId, outputTimeFn);
                stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
                @SuppressWarnings("unchecked")
                FlinkWatermarkStateInternalImpl<?> watermark = (FlinkWatermarkStateInternalImpl<?>) inMemoryState.get(namespace, stateTag);
                watermark.restoreState(reader);
                break;
            }
            case LIST: {
                StateTag stateTag = StateTags.bag(tagId, coder);
                stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
                FlinkInMemoryBag<?> bag = (FlinkInMemoryBag<?>) inMemoryState.get(namespace, stateTag);
                bag.restoreState(reader);
                break;
            }
            case ACCUMULATOR: {
                @SuppressWarnings("unchecked")
                StateTag<K, CombiningValueStateInternal<?, ?, ?>> stateTag = StateTags.keyedCombiningValue(tagId, (Coder) coder, combineFn);
                stateTag = isSystemTag ? StateTags.makeSystemTagInternal(stateTag) : stateTag;
                @SuppressWarnings("unchecked")
                FlinkInMemoryKeyedCombiningValue<?, ?, ?> combiningValue =
                        (FlinkInMemoryKeyedCombiningValue<?, ?, ?>) inMemoryState.get(namespace, stateTag);
                combiningValue.restoreState(reader);
                break;
            }
            default:
                throw new RuntimeException("Unknown State Type " + stateItemType + ".");
        }
    }

    private ByteString encodeKey(StateNamespace namespace, StateTag<? super K, ?> address) {
        return ByteString.copyFromUtf8(namespace.stringKey() + "+" + address.getId());
    }

    private int getNoOfElements() {
        int noOfElements = 0;
        for (State state : inMemoryState.values()) {
            if (!(state instanceof CheckpointableIF)) {
                throw new RuntimeException("State Implementations used by the " +
                        "Flink Dataflow Runner should implement the CheckpointableIF interface.");
            }

            if (((CheckpointableIF) state).shouldPersist()) {
                noOfElements++;
            }
        }
        return noOfElements;
    }

    private final class FlinkInMemoryValue<T> implements ValueState<T>, CheckpointableIF {

        private final ByteString stateKey;
        private final Coder<T> elemCoder;

        private T value = null;

        public FlinkInMemoryValue(ByteString stateKey, Coder<T> elemCoder) {
            this.stateKey = stateKey;
            this.elemCoder = elemCoder;
        }

        @Override
        public void clear() {
            value = null;
        }

        @Override
        public StateContents<T> get() {
            return new StateContents<T>() {
                @Override
                public T read() {
                    return value;
                }
            };
        }

        @Override
        public void set(T input) {
            this.value = input;
        }

        @Override
        public boolean shouldPersist() {
            return value != null;
        }

        @Override
        public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
            if (value != null) {

                // serialize the coder.
                byte[] coder = InstantiationUtil.serializeObject(elemCoder);

                // encode the value into a ByteString
                ByteString.Output stream = ByteString.newOutput();
                elemCoder.encode(value, stream, Coder.Context.OUTER);
                ByteString data = stream.toByteString();

                checkpointBuilder.addValueBuilder()
                        .setTag(stateKey)
                        .setData(coder)
                        .setData(data);
            }
        }

        public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
            ByteString valueContent = checkpointReader.getData();
            T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
            set(outValue);
        }
    }

    private final class FlinkWatermarkStateInternalImpl<W extends BoundedWindow>
            implements WatermarkStateInternal<W>, CheckpointableIF {

        private final ByteString stateKey;

        private Instant minimumHold = null;

        private OutputTimeFn<? super W> outputTimeFn;

        public FlinkWatermarkStateInternalImpl(ByteString stateKey, OutputTimeFn<? super W> outputTimeFn) {
            this.stateKey = stateKey;
            this.outputTimeFn = outputTimeFn;
        }

        @Override
        public void clear() {
            // Even though we're clearing we can't remove this from the in-memory state map, since
            // other users may already have a handle on this WatermarkBagInternal.
            minimumHold = null;
            watermarkHoldAccessor = null;
        }

        @Override
        public StateContents<Instant> get() {
            return new StateContents<Instant>() {
                @Override
                public Instant read() {
                    return minimumHold;
                }
            };
        }

        @Override
        public void add(Instant watermarkHold) {
            if (minimumHold == null || minimumHold.isAfter(watermarkHold)) {
                watermarkHoldAccessor = watermarkHold;
                minimumHold = watermarkHold;
            }
        }

        @Override
        public StateContents<Boolean> isEmpty() {
            return new StateContents<Boolean>() {
                @Override
                public Boolean read() {
                    return minimumHold == null;
                }
            };
        }

        @Override
        public OutputTimeFn<? super W> getOutputTimeFn() {
            return outputTimeFn;
        }

        @Override
        public String toString() {
            return Objects.toString(minimumHold);
        }

        @Override
        public boolean shouldPersist() {
            return minimumHold != null;
        }

        @Override
        public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
            if (minimumHold != null) {
                checkpointBuilder.addWatermarkHoldsBuilder()
                        .setTag(stateKey)
                        .setTimestamp(minimumHold);
            }
        }

        public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
            Instant watermark = checkpointReader.getTimestamp();
            add(watermark);
        }
    }

    private final class FlinkInMemoryKeyedCombiningValue<InputT, AccumT, OutputT>
            implements CombiningValueStateInternal<InputT, AccumT, OutputT>, CheckpointableIF {

        private final ByteString stateKey;
        private final Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn;
        private final Coder<AccumT> accumCoder;

        private AccumT accum;
        private boolean isCleared = true;

        private FlinkInMemoryKeyedCombiningValue(ByteString stateKey,
                                            Combine.KeyedCombineFn<? super K, InputT, AccumT, OutputT> combineFn,
                                            Coder<AccumT> accumCoder) {
            Preconditions.checkNotNull(combineFn);
            Preconditions.checkNotNull(accumCoder);

            this.stateKey = stateKey;
            this.combineFn = combineFn;
            this.accumCoder = accumCoder;
            accum = combineFn.createAccumulator(key);
        }

        @Override
        public void clear() {
            accum = combineFn.createAccumulator(key);
            isCleared = true;
        }

        @Override
        public StateContents<OutputT> get() {
            return new StateContents<OutputT>() {
                @Override
                public OutputT read() {
                    return combineFn.extractOutput(key, accum);
                }
            };
        }

        @Override
        public void add(InputT input) {
            isCleared = false;
            accum = combineFn.addInput(key, accum, input);
        }

        @Override
        public StateContents<AccumT> getAccum() {
            return new StateContents<AccumT>() {
                @Override
                public AccumT read() {
                    return accum;
                }
            };
        }

        @Override
        public StateContents<Boolean> isEmpty() {
            return new StateContents<Boolean>() {
                @Override
                public Boolean read() {
                    return isCleared;
                }
            };
        }

        @Override
        public void addAccum(AccumT accum) {
            isCleared = false;
            this.accum = combineFn.mergeAccumulators(key, Arrays.asList(this.accum, accum));
        }

        @Override
        public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
            return combineFn.mergeAccumulators(key, accumulators);
        }

        @Override
        public boolean shouldPersist() {
            return accum != null;
        }

        @Override
        public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
            if (accum != null) {

                // serialize the coder.
                byte[] coder = InstantiationUtil.serializeObject(accumCoder);

                // encode the accumulator into a ByteString
                ByteString.Output stream = ByteString.newOutput();
                accumCoder.encode(accum, stream, Coder.Context.OUTER);
                ByteString data = stream.toByteString();

                // put the flag that the next serialized element is an accumulator
                checkpointBuilder.addAccumulatorBuilder()
                        .setTag(stateKey)
                        .setData(coder)
                        .setData(data);
            }
        }

        public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
            ByteString valueContent = checkpointReader.getData();
            AccumT accum = this.accumCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
            addAccum(accum);
        }
    }

    private static final class FlinkInMemoryBag<T> implements BagState<T>, CheckpointableIF {
        private final List<T> contents = new ArrayList<>();

        private final ByteString stateKey;
        private final Coder<T> elemCoder;

        public FlinkInMemoryBag(ByteString stateKey, Coder<T> elemCoder) {
            this.stateKey = stateKey;
            this.elemCoder = elemCoder;
        }

        @Override
        public void clear() {
            contents.clear();
        }

        @Override
        public StateContents<Iterable<T>> get() {
            return new StateContents<Iterable<T>>() {
                @Override
                public Iterable<T> read() {
                    return contents;
                }
            };
        }

        @Override
        public void add(T input) {
            contents.add(input);
        }

        @Override
        public StateContents<Boolean> isEmpty() {
            return new StateContents<Boolean>() {
                @Override
                public Boolean read() {
                    return contents.isEmpty();
                }
            };
        }

        @Override
        public boolean shouldPersist() {
            return !contents.isEmpty();
        }

        @Override
        public void persistState(StateCheckpointWriter checkpointBuilder) throws IOException {
            if (!contents.isEmpty()) {
                // serialize the coder.
                byte[] coder = InstantiationUtil.serializeObject(elemCoder);

                checkpointBuilder.addListUpdatesBuilder()
                        .setTag(stateKey)
                        .setData(coder)
                        .writeInt(contents.size());

                for (T item : contents) {
                    // encode the element
                    ByteString.Output stream = ByteString.newOutput();
                    elemCoder.encode(item, stream, Coder.Context.OUTER);
                    ByteString data = stream.toByteString();

                    // add the data to the checkpoint.
                    checkpointBuilder.setData(data);
                }
            }
        }

        public void restoreState(StateCheckpointReader checkpointReader) throws IOException {
            int noOfValues = checkpointReader.getInt();
            for (int j = 0; j < noOfValues; j++) {
                ByteString valueContent = checkpointReader.getData();
                T outValue = elemCoder.decode(new ByteArrayInputStream(valueContent.toByteArray()), Coder.Context.OUTER);
                add(outValue);
            }
        }
    }
}
