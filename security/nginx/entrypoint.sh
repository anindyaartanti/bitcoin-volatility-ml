#!/bin/sh
set -e

echo "Generating .htpasswd for user: ${NGINX_AUTH_USER}"
htpasswd -bc /etc/nginx/.htpasswd "${NGINX_AUTH_USER}" "${NGINX_AUTH_PASSWORD}"

echo "Starting nginx..."
exec nginx -g "daemon off;"
