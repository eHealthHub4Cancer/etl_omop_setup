ALTER TABLE @vocabDatabaseSchema.concept  ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
ALTER TABLE @vocabDatabaseSchema.vocabulary  ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
ALTER TABLE @vocabDatabaseSchema.domain  ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
ALTER TABLE @vocabDatabaseSchema.concept_class  ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
ALTER TABLE @vocabDatabaseSchema.relationship  ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);