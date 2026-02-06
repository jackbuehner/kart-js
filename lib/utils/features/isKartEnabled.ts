import { hasFeatureId, isGeoJsonFeature, type GeometryWithCrs, type KartEnabledFeature } from './index.ts';

/**
 * Whether the given feature is a Kart-enabled feature.
 */
export function isKartEnabledFeature(toCheck: unknown): toCheck is KartEnabledFeature<GeometryWithCrs> {
  return (
    isGeoJsonFeature(toCheck) &&
    hasFeatureId(toCheck) &&
    typeof toCheck.id === 'string' &&
    '_kart' in toCheck &&
    typeof toCheck._kart === 'object' &&
    toCheck._kart !== null &&
    'ids' in toCheck._kart &&
    typeof toCheck._kart.ids === 'object' &&
    toCheck._kart.ids !== null &&
    Object.keys(toCheck._kart.ids).length > 0 &&
    Object.keys(toCheck._kart.ids).every((key) => typeof key === 'string') &&
    'eid' in toCheck._kart &&
    typeof toCheck._kart.eid === 'string' &&
    'geometryColumn' in toCheck._kart &&
    typeof toCheck._kart.geometryColumn === 'object' &&
    toCheck._kart.geometryColumn !== null &&
    'id' in toCheck._kart.geometryColumn &&
    typeof toCheck._kart.geometryColumn.id === 'string' &&
    'name' in toCheck._kart.geometryColumn &&
    typeof toCheck._kart.geometryColumn.name === 'string'
  );
}
