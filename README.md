
# System Info Producer with Kafka and Docker

![Screenshot 2025-03-16 231918](https://github.com/user-attachments/assets/64b78af5-66d1-446e-9da0-cbfcd1baa569)


This repository contains a Python script that collects system information and publishes it to a Kafka topic. The project uses Docker to set up the necessary services, including Kafka, Zookeeper, and Apache Pinot.

## Project Structure

- `py/produce_system_info.py`: Python script that collects system information and sends it to Kafka.
- `docker-compose.yml`: Docker Compose file to set up Kafka, Zookeeper, Apache Pinot, and Python containers.

## Prerequisites

- Docker
- Docker Compose

## Setup and Run

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Start the services:**

   ```bash
   docker-compose up
   ```

   This command starts the Kafka, Zookeeper, Pinot, and Python containers.

3. **Verify Kafka and Pinot services:**

   - Kafka: `localhost:9092` (inside the Docker network) and `localhost:29092` (for local access)
   - Apache Pinot: `http://localhost:9000` for the Pinot controller and `http://localhost:8000` for the Pinot server

4. **Check logs to ensure everything is working:**

   You can view logs for the services using:

   ```bash
   docker-compose logs -f
   ```

 docker exec pinot bash -c "/opt/pinot/bin/pinot-admin.sh AddTable -tableConfigFile /opt/pinot/def-table.json -schemaFile /opt/pinot/def-schema.json -exec"

5. **Verify the Python producer:**

   The `system-info-producer` container runs the `produce_system_info.py` script, which sends system metrics to the Kafka topic `system-info-topic`.

## Docker Compose File

The `docker-compose.yml` file defines the following services:

- **Zookeeper**: Required for Kafka to manage distributed messaging.
- **Kafka**: The message broker that receives and stores messages.
- **Pinot**: Used for OLAP analytics (optional based on your needs).
- **Python Containers**:
  - **kafka-producer-py**: (Optional) For any additional Kafka producer tasks.
  - **system-info-producer**: Runs the system information producer script.

## Notes

- Adjust the Docker Compose configurations as needed for your environment.
- Ensure that the `produce_system_info.py` script and `docker-compose.yml` file paths are correctly set up.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


