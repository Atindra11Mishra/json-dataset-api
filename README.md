📚 JSON Dataset Management API - Complete Documentation🎯 Project OverviewWhat It DoesA high-performance REST API for managing JSON datasets with advanced querying capabilities. Built as a backend assignment for FreightFox, this API enables:
Insert: Store JSON records in named datasets with automatic validation
Group-By: Group records by any field with intelligent handling of missing/null values
Sort-By: Sort records by any field (numeric, string, boolean) with configurable order
Combined Operations: Group records, then sort within each group
Real-world Use Cases:

Logistics data aggregation (shipments by status, sorted by date)
User analytics (group by region, sort by activity)
Dynamic reporting (flexible grouping and sorting without predefined schemas)
🛠️ Tech StackLayerTechnologyVersionPurposeLanguageJava21Modern Java featuresFrameworkSpring Boot3.2.2REST API, DI, auto-configurationDatabasePostgreSQL (Supabase)15+JSONB column for flexible storageORMHibernate/JPA6.3+Object-relational mappingBuild ToolMaven3.9+Dependency managementTestingJUnit 5 + Mockito5.10+Unit & integration testsJSON ProcessingJackson2.15+Serialization/deserializationDatabase DriverPostgreSQL JDBC42.7.1Database connectivity