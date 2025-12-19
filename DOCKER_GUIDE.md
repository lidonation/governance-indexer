# Docker Quick Start Guide

## 🚀 Start Everything

```bash
cd /Users/tyty/Desktop/work/governance-indexer/rust-indexer

# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f indexer
```

## 📊 Check Status

```bash
# Check running containers
docker-compose ps

# View indexer logs
docker-compose logs -f indexer

# View database logs
docker-compose logs -f postgres
```

## 🗄️ Database Access

```bash
# Connect to database
docker-compose exec postgres psql -U govindex -d govindex

# Inside psql:
# \dt                          -- List tables
# SELECT * FROM drep_timeline_event LIMIT 10;
# \q                           -- Quit
```

## 🛑 Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (reset database)
docker-compose down -v
```

## 🔧 Development Workflow

```bash
# Rebuild after code changes
docker-compose build indexer
docker-compose up -d indexer

# View real-time logs
docker-compose logs -f indexer

# Restart a service
docker-compose restart indexer
```

## 📝 Useful SQL Queries

```sql
-- Count events by type
SELECT type, COUNT(*) 
FROM drep_timeline_event 
GROUP BY type;

-- Recent events
SELECT * 
FROM drep_timeline_event 
ORDER BY timestamp DESC 
LIMIT 20;

-- Events for specific DRep
SELECT * 
FROM drep_timeline_event 
WHERE drep_view = 'YOUR_DREP_ID';
```

## 🐛 Troubleshooting

### Database won't start
```bash
# Check if port 5433 is available
lsof -i :5433

# Remove volumes and restart
docker-compose down -v
docker-compose up -d
```

### Indexer can't connect to database
```bash
# Check database health
docker-compose ps

# Ensure database is ready
docker-compose logs postgres

# Restart indexer after database is ready
docker-compose restart indexer
```

docker-compose logs --tail=100
```

## 🚢 Multi-Platform Production Build

To build for **both** standard servers (AMD64) and Macs/ARM servers (ARM64) simultaneously, use `docker buildx`. This creates a single image tag that works everywhere.

```bash
# 1. Create a new builder instance (only needed once)
docker buildx create --use

# 2. Build and push for multiple platforms
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag registry.2lovelaces.io/voltaire/www.1694.io/rust-indexer:latest \
  --push \
  .
```

> [!NOTE]
> Multi-platform builds **must** be pushed to a registry immediately (using `--push`) because the local Docker daemon cannot hold multiple architectures for the same tag simultaneously.
