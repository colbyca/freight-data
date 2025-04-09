// Adapted from code by 
// https://github.com/LockBlock-dev/react-leaflet-heat-layer

import {
    createElementObject,
    createLayerComponent,
    updateGridLayer,
    type LayerProps,
    type LeafletContextInterface,
} from "@react-leaflet/core";
import L, { HeatLatLngTuple, LatLng } from "leaflet"
import "leaflet.heat";

export interface HeatLayerProps extends LayerProps, L.HeatMapOptions {
    latlngs: Array<LatLng | HeatLatLngTuple>;
}

const createHeatLayer = (
    { latlngs, ...options }: HeatLayerProps,
    context: LeafletContextInterface
) => {
    const layer = L.heatLayer(latlngs, options);
    return createElementObject(layer, context);
};

const updateHeatLayer = (
    layer: L.HeatLayer,
    { latlngs, ...options }: HeatLayerProps,
    prevProps: HeatLayerProps
) => {
    layer.setLatLngs(latlngs);
    layer.setOptions(options);

    updateGridLayer(layer, options, prevProps);
};

export default createLayerComponent<L.HeatLayer, HeatLayerProps>(
    createHeatLayer,
    updateHeatLayer
);