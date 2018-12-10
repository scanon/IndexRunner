#!/bin/sh

BASE=./scripts/data
IN=./work/input.json

if [ $(grep -c schema $IN) -gt 0 ] ; then
  EXT="_schema.json" 
fi

cat $IN
if [ $(grep -c kb_GenomeIndexer.genome_index $IN) -gt 0 ] ; then
   FILE=genome.json
elif [ $(grep -c kb_GenomeIndexer.genome_mapping $IN) -gt 0 ] ; then
   FILE=genome_schema.json
elif [ $(grep -c kb_GenomeIndexer.genomefeature_index $IN) -gt 0 ] ; then
   FILE=genomefeature.json
elif [ $(grep -c kb_GenomeIndexer.genomefeature_mapping $IN) -gt 0 ] ; then
   FILE=genomefeature_schema.json
elif [ $(grep -c NarrativeIndexer.index $IN) -gt 0 ] ; then
   FILE=narrative.json
elif [ $(grep -c NarrativeIndexer.mapping $IN) -gt 0 ] ; then
   FILE=narrative_schema.json
else
  echo "Error"
  FILE=error.json
  cp ${BASE}/${FILE} /kb/module/work/output.json
  exit 1
fi
P="${BASE}/${FILE}"

cp $P /kb/module/work/output.json
