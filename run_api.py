#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script de lancement rapide de l'API Apple Platform
"""

import sys
import os

# Ajouter le répertoire parent au PYTHONPATH pour permettre l'import de 'api'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("🚀 Lancement de l'API Apple Platform Analytics")
    print("=" * 60)
    print()
    print("📚 Documentation Swagger : http://localhost:8000/docs")
    print("📖 Documentation ReDoc   : http://localhost:8000/redoc")
    print()
    print("🔐 Utilisateurs de test :")
    print("   - admin / admin123")
    print("   - user / user123")
    print()
    print("=" * 60)
    print()
    
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
