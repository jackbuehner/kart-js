import { hasFeatureId, isGeoJsonFeature, type GeometryWithCrs, type KartEnabledFeature } from './index.ts';

/**
 * Whether the given feature is a Kart-enabled feature.
 */
export function isKartEnabledFeature(feature: GeoJSON.Feature): feature is KartEnabledFeature<GeometryWithCrs> {
  return (
    isGeoJsonFeature(feature) &&
    hasFeatureId(feature) &&
    typeof feature.id === 'string' &&
    '_kart' in feature &&
    typeof feature._kart === 'object' &&
    feature._kart !== null &&
    'ids' in feature._kart &&
    typeof feature._kart.ids === 'object' &&
    feature._kart.ids !== null &&
    Object.keys(feature._kart.ids).length > 0 &&
    Object.keys(feature._kart.ids).every((key) => typeof key === 'string') &&
    'eid' in feature._kart &&
    typeof feature._kart.eid === 'string' &&
    'geometryColumn' in feature._kart &&
    typeof feature._kart.geometryColumn === 'object' &&
    feature._kart.geometryColumn !== null &&
    'id' in feature._kart.geometryColumn &&
    typeof feature._kart.geometryColumn.id === 'string' &&
    'name' in feature._kart.geometryColumn &&
    typeof feature._kart.geometryColumn.name === 'string'
  );
}
