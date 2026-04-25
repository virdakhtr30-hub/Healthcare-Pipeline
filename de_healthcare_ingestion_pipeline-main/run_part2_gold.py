from src.transformations.gold_transformations import GoldTransformer

transformer = GoldTransformer()
transformer.run_all()

print("Gold transformations completed.")
print("Check: data/iceberg_warehouse/gold/")
print("Check: data/transformation_metrics/")