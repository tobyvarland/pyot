from pyot.config import get_settings
from pyot.hoist import HoistAggregator, HoistExcelExporter

def main():
    app_config = get_settings()
    config = app_config.push_to_server.hoist_aggregation

    aggregator = HoistAggregator(config)
    aggregator.run()

    exporter = HoistExcelExporter(
        config.output_file,
        config.output_file.with_suffix(".xlsx"),
    )
    exporter.write()

    config.output_file.unlink()

if __name__ == "__main__":
    main()