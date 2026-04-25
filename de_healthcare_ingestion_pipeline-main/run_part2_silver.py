from src.transformations.silver_transformations import SilverTransformer

transformer = SilverTransformer()
transformer.run_all()

print("Silver transformations completed.")
print("Check: data/iceberg_warehouse/silver/")
print("Check: data/transformation_metrics/")