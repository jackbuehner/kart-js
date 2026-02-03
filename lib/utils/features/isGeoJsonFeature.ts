export function isGeoJsonFeature(toCheck: unknown): toCheck is GeoJSON.Feature {
  return (
    !!toCheck &&
    typeof toCheck === 'object' &&
    'type' in toCheck &&
    toCheck.type === 'Feature' &&
    'geometry' in toCheck &&
    'properties' in toCheck &&
    typeof toCheck.geometry === 'object' &&
    typeof toCheck.properties === 'object'
  );
}
