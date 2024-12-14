# Use an official Node runtime as a parent image
FROM node:16-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire transformer directory
COPY transformer/ .

# Install project dependencies
RUN npm install

# Create output directory
RUN mkdir -p output

# Define environment variable
ENV NODE_ENV=production

# Run the transformation script
CMD ["node", "resourceTransform.js"]