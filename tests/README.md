# pytebis Test Suite

Diese Testsuite enthält Unit- und Integrationstests für das pytebis Paket.

## Installation der Test-Abhängigkeiten

```bash
pip install -e .[dev]
```

Oder nur die Hauptabhängigkeiten:
```bash
pip install -e .
```

## Tests ausführen

Alle Tests ausführen:
```bash
pytest
```

Tests mit Ausgabe:
```bash
pytest -v
```

Tests mit Coverage-Report:
```bash
pytest --cov=pytebis --cov-report=html
```

Nur spezifische Test-Datei:
```bash
pytest tests/test_utils.py
```

Nur spezifische Test-Klasse:
```bash
pytest tests/test_utils.py::TestSelectiveMerge
```

Nur spezifischer Test:
```bash
pytest tests/test_utils.py::TestSelectiveMerge::test_merge_simple_dict
```

## Test-Struktur

- `test_utils.py` - Tests für Utility-Funktionen und Hilfsklassen
  - `TestSelectiveMerge` - Configuration Merge Funktion
  - `TestTebisMST` - MST Basisklasse
  - `TestTebisRMST` - Real MST Klasse
  - `TestTebisVMST` - Virtual MST Klasse
  - `TestTebisGroupElement` - Gruppen-Elemente
  - `TestTebisGroupMember` - Gruppen-Mitglieder
  - `TestTebisTreeElement` - Tree-Struktur
  - `TestGetDataSeriesAsJson` - JSON Konvertierung
  - `TestTebisTreeEncoder` - Custom JSON Encoder

- `test_tebis.py` - Tests für die Tebis Hauptklasse
  - `TestTebisConfiguration` - Konfigurations-Tests
  - `TestTebisTimestampConversion` - Timestamp-Konvertierung
  - `TestTebisMSTRetrieval` - MST Abruf-Methoden
  - `TestTebisOracleDBMethods` - Oracle DB Funktionen
  - `TestTebisDataCalculations` - Datenberechnungen

- `test_numpy_compatibility.py` - NumPy Kompatibilitätstests
  - `TestNumpyCompatibility` - Tests für NumPy 1.x und 2.x Kompatibilität
  - Structured Arrays
  - NaN Handling
  - dtype Definitionen
  - Array Indexing
  - Version-spezifisches Verhalten

## Continuous Integration

Die Tests werden automatisch bei jedem Push und Pull Request über GitHub Actions ausgeführt.

## Coverage

Nach dem Ausführen mit `--cov-report=html` können Sie den Coverage-Report öffnen:
```bash
open htmlcov/index.html
```
