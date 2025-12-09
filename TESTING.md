# Test-Abdeckung Zusammenfassung

## Übersicht

Die Test-Suite für pytebis enthält **48 Unit- und Integrationstests**, die verschiedene Aspekte des Pakets abdecken.

### Test-Dateien

1. **tests/test_utils.py** (20 Tests)
   - Utility-Funktionen und Hilfsklassen
   
2. **tests/test_tebis.py** (18 Tests)
   - Hauptklasse Tebis und deren Methoden

3. **tests/test_numpy_compatibility.py** (7 Tests)
   - NumPy Version 1.x und 2.x Kompatibilitätstests

4. **tests/test_numpy2_compatibility.py** (3 Tests)
   - NumPy 2.0 spezifische Breaking Changes Tests

## Getestete Komponenten

### ✅ Konfiguration & Initialisierung
- Default-Konfiguration
- Custom Port Konfiguration
- Oracle DB Auto-Enable/Disable
- LiveValues Feature

### ✅ Datenklassen
- `TebisMST` - Basis Messpunkt-Klasse
- `TebisRMST` - Reale Messpunkte
- `TebisVMST` - Virtuelle Messpunkte
- `TebisGroupElement` - Gruppen
- `TebisGroupMember` - Gruppen-Mitglieder
- `TebisTreeElement` - Tree-Struktur mit Suche
- `TebisMapTreeGroup` - Tree-Mapping

### ✅ Hilfsfunktionen
- `selective_merge()` - Konfigurations-Merge (3 Tests)
- `getDataSeries_as_Json()` - JSON-Konvertierung (2 Tests)
- `tebisTreeEncoder` - Custom JSON Encoder (3 Tests)
- `testUnicodeError()` - Unicode Fehlerbehandlung

### ✅ Messpunkt-Verwaltung
- MST Abruf per ID
- MST Abruf per Name
- Mehrere MSTs abrufen
- Nicht-existierende MSTs

### ✅ Zeitstempel-Konvertierung
- DateTime zu Timestamp
- Float zu Timestamp (Sekunden → Millisekunden)
- Integer zu Timestamp
- Erkennung bereits konvertierter Timestamps

### ✅ Datenberechnung
- Zeitbereich-Kalkulation
- Sample-Count-Berechnung
- Minimum Sample Count (mindestens 1)

### ✅ Oracle DB Funktionen
- Exception bei fehlender DB-Verbindung
- Tree Group Abruf mit Oracle

### ✅ NumPy Kompatibilität
- Structured Array Creation
- NaN Handling
- dtype Kompatibilität
- Array Indexing
- tolist() Konvertierung
- NumPy 1.x spezifisches Verhalten
- NumPy 2.x spezifisches Verhalten

### ✅ NumPy 2.0 Breaking Changes
- `np.str_` statt `np.unicode_` (behoben)
- String dtype Tests
- Mixed dtype Arrays
- Structured Arrays mit Strings

**Hinweis**: In NumPy 2.0 wurde `np.unicode_` entfernt. Alle Vorkommen wurden durch `np.str_` ersetzt. Details siehe [NUMPY2_MIGRATION.md](NUMPY2_MIGRATION.md)

## NumPy Version Test-Matrix

Das Projekt wird automatisch gegen verschiedene NumPy-Versionen getestet:

### NumPy 1.x (< 2.0)
- Python 3.8 + NumPy 1.x
- Python 3.9 + NumPy 1.x
- Python 3.10 + NumPy 1.x
- Python 3.11 + NumPy 1.x

### NumPy 2.x (>= 2.0)
- Python 3.9 + NumPy 2.x
- Python 3.10 + NumPy 2.x
- Python 3.11 + NumPy 2.x
- Python 3.12 + NumPy 2.x

**Hinweis**: NumPy 2.0+ erfordert Python 3.9+

## Coverage-Bericht

```
Name                    Stmts   Miss  Cover
-------------------------------------------
pytebis/__init__.py         1      0   100%
pytebis/lazyloader.py      21     11    48%
pytebis/tebis.py          844    654    23%
-------------------------------------------
TOTAL                     866    665    23%
```

**Hinweis**: Die relativ niedrige Coverage liegt daran, dass die Tests aktuell keine Live-Server-Verbindungen testen (Socket-Kommunikation, Daten-Download, etc.). Die Kern-Logik und Utility-Funktionen sind gut abgedeckt.

## Zukünftige Erweiterungen

Mögliche zusätzliche Tests:
- [ ] Mock-basierte Socket-Kommunikation Tests
- [ ] Pandas DataFrame Konvertierung
- [ ] Binär-Daten Parsing
- [ ] Oracle DB Queries (mit Mock-DB)
- [ ] LiveValues Feature
- [ ] Error Handling für Netzwerk-Fehler
- [ ] Performance Tests für große Datenmengen

## CI/CD Integration

Die Tests werden automatisch ausgeführt:
- Bei jedem Push auf `master`, `main`, `develop`
- Bei jedem Pull Request
- **Mit NumPy 1.x**: Python 3.8, 3.9, 3.10, 3.11
- **Mit NumPy 2.x**: Python 3.9, 3.10, 3.11, 3.12
- Mit separatem Coverage-Upload für jede NumPy-Version
