# =============================================================================
# Laravel app + RoadRunner (Octane) Dockerfile
# Base: PHP 8.3 FPM image. We add the rdkafka extension and RoadRunner binary.
# =============================================================================
FROM php:8.3-cli AS base

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    zip \
    unzip \
    libzip-dev \
    librdkafka-dev \
    && docker-php-ext-install zip pdo pdo_mysql pcntl \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka \
    && rm -rf /var/lib/apt/lists/*

# Install Composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

# Copy composer files first for layer caching
COPY composer.json composer.lock ./

# Install PHP dependencies (no dev, optimised autoloader)
RUN composer install --no-dev --optimize-autoloader --no-interaction --ignore-platform-reqs

# Copy application source
COPY . .

# Install RoadRunner binary via the Octane artisan command
RUN php artisan octane:install --server=roadrunner --no-interaction 2>/dev/null || \
    ./vendor/bin/rr get-binary 2>/dev/null || true

# Permissions
RUN chown -R www-data:www-data storage bootstrap/cache \
    && chmod -R 775 storage bootstrap/cache

EXPOSE 8000
