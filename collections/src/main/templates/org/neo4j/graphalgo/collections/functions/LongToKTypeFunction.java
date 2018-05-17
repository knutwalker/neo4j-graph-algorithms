package org.neo4j.graphalgo.collections.functions;

/**
 * A function that applies <code>KType</code> to <code>long</code>.
 */
/*! ${TemplateOptions.generatedAnnotation} !*/
@FunctionalInterface
public interface LongToKTypeFunction<KType> {
    public KType apply(long value);
}
