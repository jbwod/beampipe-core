#!/usr/bin/env python3
"""
FastAPI Boilerplate Setup Script

Automates copying the correct configuration files for different deployment scenarios.
"""

import shutil
import sys
from pathlib import Path

DEPLOYMENTS = {
    "local": {
        "name": "Local development with Uvicorn",
        "description": "Auto-reload enabled, development-friendly",
        "path": "scripts/local_with_uvicorn",
    },
    "staging": {
        "name": "Staging with Gunicorn managing Uvicorn workers",
        "description": "Production-like setup for testing",
        "path": "scripts/gunicorn_managing_uvicorn_workers",
    },
    "production": {
        "name": "Production with NGINX",
        "description": "Full production setup with reverse proxy",
        "path": "scripts/production_with_nginx",
    },
}


def show_help():
    """Display help information"""
    print("FastAPI Boilerplate Setup")
    print("=" * 25)
    print()
    print("Usage: python setup.py <deployment-type>")
    print()
    print("Available deployment types:")
    for key, config in DEPLOYMENTS.items():
        print(f"  {key:12} - {config['name']}")
        print(f"  {' ' * 12}   {config['description']}")
        print()
    print("Examples:")
    print("  python setup.py local       # Set up for local development")
    print("  python setup.py staging     # Set up for staging environment")
    print("  python setup.py production  # Set up for production deployment")


def copy_files(deployment_type: str):
    """Copy configuration files for the specified deployment type"""
    if deployment_type not in DEPLOYMENTS:
        print(f"❌ Unknown deployment type: {deployment_type}")
        print()
        show_help()
        return False

    config = DEPLOYMENTS[deployment_type]
    source_path = Path(config["path"])

    if not source_path.exists():
        print(f"❌ Configuration path not found: {source_path}")
        return False

    print(f"🚀 Setting up {config['name']}...")
    print(f"   {config['description']}")
    print()

    files_to_copy = [
        ("Dockerfile", "Dockerfile"),
        ("docker-compose.yml", "docker-compose.yml"),
        (".env.example", "src/.env"),
    ]

    success = True
    for source_file, dest_file in files_to_copy:
        source = source_path / source_file
        dest = Path(dest_file)

        if not source.exists():
            print(f"⚠️  Warning: {source} not found, skipping...")
            continue

        try:
            dest.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(source, dest)
            print(f"✅ Copied {source} → {dest}")

        except Exception as e:
            print(f"❌ Failed to copy {source} → {dest}: {e}")
            success = False

    if success:
        print()
        print("🎉 Setup complete!")
        print()

        if deployment_type in ["staging", "production"]:
            print("⚠️  IMPORTANT: Update the .env file with your production values:")
            print("   - Generate a new SECRET_KEY: openssl rand -hex 32")
            print("   - Change all passwords and sensitive values")
            print()

        print("Next steps:")
        print("   docker compose up")

        if deployment_type == "local":
            print("   open http://127.0.0.1:8000/docs")
        elif deployment_type == "production":
            print("   open http://localhost")

        return True

    return False


def interactive_setup():
    """Interactive setup when no arguments provided"""
    print("FastAPI Boilerplate Setup")
    print("=" * 25)
    print()
    print("Choose your deployment type:")
    print()

    options = list(DEPLOYMENTS.keys())
    for i, key in enumerate(options, 1):
        config = DEPLOYMENTS[key]
        print(f"  {i}. {config['name']}")
        print(f"     {config['description']}")
        print()

    while True:
        try:
            choice = input(f"Enter your choice (1-{len(options)}): ").strip()

            if choice.isdigit():
                choice_num = int(choice)
                if 1 <= choice_num <= len(options):
                    return options[choice_num - 1]

            if choice.lower() in DEPLOYMENTS:
                return choice.lower()

            print(f"❌ Invalid choice. Please enter 1-{len(options)} or the deployment name.")

        except KeyboardInterrupt:
            print("\n\n👋 Setup cancelled.")
            return None
        except EOFError:
            print("\n\n👋 Setup cancelled.")
            return None


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help", "help"]:
        show_help()
        return

    if len(sys.argv) == 2:
        deployment_type = sys.argv[1].lower()
    elif len(sys.argv) == 1:
        deployment_type = interactive_setup()
        if deployment_type is None:
            return
    else:
        show_help()
        return

    success = copy_files(deployment_type)

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
