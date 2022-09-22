/*
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
package io.trino.operator.aggregation;

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.FunctionNullability;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.AggregationNode.Step;

import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Aggregator
{
    private final Accumulator accumulator;
    private final Step step;
    private final Type intermediateType;
    private final Type finalType;
    private final int[] inputChannels;
    private final OptionalInt maskChannel;
    private final FunctionNullability functionNullability;
    private final AggregationMask mask = AggregationMask.createSelectAll(0);

    public Aggregator(
            Accumulator accumulator,
            Step step,
            Type intermediateType,
            Type finalType,
            List<Integer> inputChannels,
            OptionalInt maskChannel,
            FunctionNullability functionNullability)
    {
        this.accumulator = requireNonNull(accumulator, "accumulator is null");
        this.step = requireNonNull(step, "step is null");
        this.intermediateType = requireNonNull(intermediateType, "intermediateType is null");
        this.finalType = requireNonNull(finalType, "finalType is null");
        this.inputChannels = Ints.toArray(requireNonNull(inputChannels, "inputChannels is null"));
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.functionNullability = requireNonNull(functionNullability, "functionNullability is null");
        checkArgument(step.isInputRaw() || inputChannels.size() == 1, "expected 1 input channel for intermediate aggregation");
    }

    public Type getType()
    {
        if (step.isOutputPartial()) {
            return intermediateType;
        }
        return finalType;
    }

    public void processPage(Page page)
    {
        if (step.isInputRaw()) {
            mask.reset(page.getPositionCount());
            if (maskChannel.isPresent()) {
                mask.applyMaskBlock(page.getBlock(maskChannel.getAsInt()));
            }
            Page arguments = page.getColumns(inputChannels);
            for (int channel = 0; channel < arguments.getChannelCount(); channel++) {
                if (!functionNullability.isArgumentNullable(channel)) {
                    mask.unselectNullPositions(arguments.getBlock(channel));
                }
            }
            if (mask.isSelectNone()) {
                return;
            }

            accumulator.addInput(arguments, mask);
        }
        else {
            accumulator.addIntermediate(page.getBlock(inputChannels[0]));
        }
    }

    public void evaluate(BlockBuilder blockBuilder)
    {
        if (step.isOutputPartial()) {
            accumulator.evaluateIntermediate(blockBuilder);
        }
        else {
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    public long getEstimatedSize()
    {
        return accumulator.getEstimatedSize();
    }
}
