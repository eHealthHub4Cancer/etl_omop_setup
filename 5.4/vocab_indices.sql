/**************************
Standardized vocabularies
***************************/
CREATE INDEX idx_concept_concept_id  ON @vocabDatabaseSchema.concept  (concept_id ASC);
CLUSTER @vocabDatabaseSchema.concept  USING idx_concept_concept_id ;
CREATE INDEX idx_concept_code ON @vocabDatabaseSchema.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON @vocabDatabaseSchema.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON @vocabDatabaseSchema.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON @vocabDatabaseSchema.concept (concept_class_id ASC);
CREATE INDEX idx_vocabulary_vocabulary_id  ON @vocabDatabaseSchema.vocabulary  (vocabulary_id ASC);
CLUSTER @vocabDatabaseSchema.vocabulary  USING idx_vocabulary_vocabulary_id ;
CREATE INDEX idx_domain_domain_id  ON @vocabDatabaseSchema.domain  (domain_id ASC);
CLUSTER @vocabDatabaseSchema.domain  USING idx_domain_domain_id ;
CREATE INDEX idx_concept_class_class_id  ON @vocabDatabaseSchema.concept_class  (concept_class_id ASC);
CLUSTER @vocabDatabaseSchema.concept_class  USING idx_concept_class_class_id ;
CREATE INDEX idx_concept_relationship_id_1  ON @vocabDatabaseSchema.concept_relationship  (concept_id_1 ASC);
CLUSTER @vocabDatabaseSchema.concept_relationship  USING idx_concept_relationship_id_1 ;
CREATE INDEX idx_concept_relationship_id_2 ON @vocabDatabaseSchema.concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON @vocabDatabaseSchema.concept_relationship (relationship_id ASC);
CREATE INDEX idx_relationship_rel_id  ON @vocabDatabaseSchema.relationship  (relationship_id ASC);
CLUSTER @vocabDatabaseSchema.relationship  USING idx_relationship_rel_id ;
CREATE INDEX idx_concept_synonym_id  ON @vocabDatabaseSchema.concept_synonym  (concept_id ASC);
CLUSTER @vocabDatabaseSchema.concept_synonym  USING idx_concept_synonym_id ;
CREATE INDEX idx_concept_ancestor_id_1  ON @vocabDatabaseSchema.concept_ancestor  (ancestor_concept_id ASC);
CLUSTER @vocabDatabaseSchema.concept_ancestor  USING idx_concept_ancestor_id_1 ;
CREATE INDEX idx_concept_ancestor_id_2 ON @vocabDatabaseSchema.concept_ancestor (descendant_concept_id ASC);
CREATE INDEX idx_source_to_concept_map_3  ON @vocabDatabaseSchema.source_to_concept_map  (target_concept_id ASC);
CLUSTER @vocabDatabaseSchema.source_to_concept_map  USING idx_source_to_concept_map_3 ;
CREATE INDEX idx_source_to_concept_map_1 ON @vocabDatabaseSchema.source_to_concept_map (source_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_2 ON @vocabDatabaseSchema.source_to_concept_map (target_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_c ON @vocabDatabaseSchema.source_to_concept_map (source_code ASC);
CREATE INDEX idx_drug_strength_id_1  ON @vocabDatabaseSchema.drug_strength  (drug_concept_id ASC);
CLUSTER @vocabDatabaseSchema.drug_strength  USING idx_drug_strength_id_1 ;
CREATE INDEX idx_drug_strength_id_2 ON @vocabDatabaseSchema.drug_strength (ingredient_concept_id ASC);