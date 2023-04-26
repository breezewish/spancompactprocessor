package spancompactprocessor

import (
	"context"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type spanProcessor struct {
	config Config
	logger *zap.Logger
}

// newSpanProcessor returns the span processor.
func newSpanProcessor(logger *zap.Logger, config Config) (*spanProcessor, error) {
	sp := &spanProcessor{
		config: config,
		logger: logger,
	}
	return sp, nil
}

func (sp *spanProcessor) doProcessTraces(td ptrace.Traces) (ptrace.Traces, error) {
	// Phase 1: Collect all spans
	allSpans := make([]ptrace.Span, 0)
	spanBySpanID := make(map[pcommon.SpanID]ptrace.Span)

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				allSpans = append(allSpans, span)
				spanBySpanID[span.SpanID()] = span
			}
		}
	}

	// Phase 2: Iterate all spans, build a map containing their durations and relations
	durationBySpanID := make(map[pcommon.SpanID]pcommon.Timestamp)
	childrensBySpanID := make(map[pcommon.SpanID]uint64)
	childrenSpansBySpanID := make(map[pcommon.SpanID][]ptrace.Span)

	for _, span := range allSpans {
		durationBySpanID[span.SpanID()] = span.EndTimestamp() - span.StartTimestamp()
		if !span.ParentSpanID().IsEmpty() {
			childrensBySpanID[span.ParentSpanID()] += 1
			if _, ok := childrenSpansBySpanID[span.ParentSpanID()]; !ok {
				childrenSpansBySpanID[span.ParentSpanID()] = make([]ptrace.Span, 0)
			}
			childrenSpansBySpanID[span.ParentSpanID()] = append(childrenSpansBySpanID[span.ParentSpanID()], span)
		}
	}

	// Phase 3: For each span, try to kill parent span
	spanIsKilled := make(map[pcommon.SpanID]bool)
	for _, span := range allSpans {
		spanID := span.SpanID()
		parentSpanID := span.ParentSpanID()
		if parentSpanID.IsEmpty() {
			continue
		}
		if _, ok := spanBySpanID[parentSpanID]; !ok {
			// Parent is not received
			continue
		}
		if spanIsKilled[parentSpanID] {
			continue
		}
		if spanBySpanID[parentSpanID].StartTimestamp() > span.StartTimestamp() || spanBySpanID[parentSpanID].EndTimestamp() < span.EndTimestamp() {
			// There are time offsets, skip
			continue
		}
		if durationBySpanID[parentSpanID] < durationBySpanID[spanID] {
			// Duration is incorrect
			continue
		}

		if childrensBySpanID[parentSpanID] == 1 {
			// if this is the only major child span.
			if float64(durationBySpanID[spanID])/float64(durationBySpanID[parentSpanID]) > 0.95 {
				spanIsKilled[parentSpanID] = true
				// fmt.Printf("Killed span %s\n", spanBySpanID[parentSpanID].Name())
			}
		}
		// if childrensBySpanID[parentSpanID] > 1 {
		// 	// if this is the major span and all sibling spans do not have children.
		// 	allSiblingsDoNotHaveChildren := true
		// 	for _, span := range childrenSpansBySpanID[parentSpanID] {
		// 		if childrensBySpanID[span.SpanID()] > 0 {
		// 			allSiblingsDoNotHaveChildren = false
		// 			// fmt.Printf("Excluding %s because its children has children\n", spanBySpanID[parentSpanID].Name())
		// 			// fmt.Printf("Children %s has childrens: %d\n", span.Name(), childrensBySpanID[span.SpanID()])
		// 			break
		// 		}
		// 	}

		// 	if allSiblingsDoNotHaveChildren {

		// 		durationOfAllSiblingSpans := pcommon.Timestamp(0)
		// 		for _, span := range childrenSpansBySpanID[parentSpanID] {
		// 			durationOfAllSiblingSpans += durationBySpanID[span.SpanID()]
		// 		}

		// 		fmt.Printf("Trying to kill %s, dur = %d, dur_all = %d, current_span_dur = %d, current_span = %s\n",
		// 			spanBySpanID[parentSpanID].Name(),
		// 			durationBySpanID[parentSpanID],
		// 			durationOfAllSiblingSpans,
		// 			durationBySpanID[spanID],
		// 			span.Name())

		// 		if float64(durationBySpanID[spanID])/float64(durationBySpanID[parentSpanID]) > 0.95 &&
		// 			float64(durationBySpanID[spanID])/float64(durationOfAllSiblingSpans) > 0.95 {
		// 			spanIsKilled[parentSpanID] = true
		// 			fmt.Printf("Killed span %s\n", spanBySpanID[parentSpanID].Name())
		// 		}
		// 	}
		// }
	}

	// Phase 3: For each span, try to kill parent span,

	parentIDRemap := make(map[pcommon.SpanID]pcommon.SpanID)
	for killedSpanID := range spanIsKilled {
		if spanBySpanID[killedSpanID].ParentSpanID().IsEmpty() {
			continue
		}
		parentIDRemap[killedSpanID] = spanBySpanID[killedSpanID].ParentSpanID()
	}
	// Try to resolve ID
	for id, mappedID := range parentIDRemap {
		for {
			mid, ok := parentIDRemap[mappedID]
			if !ok {
				break
			}
			mappedID = mid
		}
		parentIDRemap[id] = mappedID
	}

	// Phase 4: Remove killed spans
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			spans.RemoveIf(func(span ptrace.Span) bool {
				return spanIsKilled[span.SpanID()]
			})
		}
	}

	// Phase 5: Fix parent IDs
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if newParentID, ok := parentIDRemap[span.ParentSpanID()]; ok {
					span.SetParentSpanID(newParentID)
				}
			}
		}
	}

	sp.logger.Info("Compacted spans", zap.Int("all_spans", len(allSpans)), zap.Int("killed_spans", len(spanIsKilled)))

	return td, nil
}

func (sp *spanProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	var err error
	td, err = sp.doProcessTraces(td)
	if err != nil {
		return td, err
	}
	// td, err = sp.doProcessTraces(td)
	// if err != nil {
	// 	return td, err
	// }
	// td, err = sp.doProcessTraces(td)
	// if err != nil {
	// 	return td, err
	// }
	// td, err = sp.doProcessTraces(td)
	// if err != nil {
	// 	return td, err
	// }
	return td, nil
}
