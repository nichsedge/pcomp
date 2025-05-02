# pcomp

![dbt-dag](https://github.com/user-attachments/assets/336b232a-dde7-4358-b338-d1cbff179483)

![image](https://github.com/user-attachments/assets/8ba813b3-31e5-4552-a586-31429cb65e19)

`pcomp` is a tool inspired by [PCPartPicker](https://pcpartpicker.com/), designed specifically for the Indonesian market. This repository focuses on scraping computer hardware prices from local Indonesian retailers, currently supported by **EnterKomputer** and **Viraindo**, and storing the data in BigQuery for further analysis and processing.

## Features

- **Automated Scraping**: Weekly automated scraping of hardware prices using GitHub Actions.
- **Data Storage**: Scraped data is stored in BigQuery for analysis and reporting.
- **Data Transformation**: Built-in [dbt](https://www.getdbt.com/) project for transforming and organizing data for analytics.

## Tech Stack

- **Python**: For web scraping and data processing.
- **GitHub Actions**: For automation of the scraping process, scheduled to run weekly.
- **BigQuery**: For storing scraped data.
- **dbt**: For transforming and modeling data stored in BigQuery.

## Automation

The scraping process is automated using GitHub Actions. The workflow is configured to run every week, scrape the latest prices from the supported websites, and upload the data to BigQuery.

## Data Transformation

Data transformation is handled using dbt, which processes the raw data stored in BigQuery into models that are easy to query and analyze.

## License

This project is licensed under the MIT License.

## Acknowledgements

- **PCPartPicker**: The original inspiration for this project.
- **EnterKomputer** & **Viraindo**: The current data sources.
