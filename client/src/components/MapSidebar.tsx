import React from "react";

export enum MapViewType {
    NONE = 0,
    STOPS = 1,
    ROUTES = 2,
    HEAT = 3,
}


export interface SidebarProps {
    selectedMode: MapViewType;
    setSelectedMode: React.Dispatch<React.SetStateAction<MapViewType>>;
}



export const MapSidebar = ({selectedMode, setSelectedMode} : SidebarProps) => {

    return (
        <div id="map-sidebar">
            <SelectModeButton text={"Truck Stops"}
                              disabled={selectedMode === MapViewType.STOPS}
                              onclick={() => setSelectedMode(MapViewType.STOPS)} />
            <SelectModeButton text={"Routes"}
                              disabled={selectedMode === MapViewType.ROUTES}
                              onclick={() => setSelectedMode(MapViewType.ROUTES)} />
            <SelectModeButton text={"Heatmap"}
                              disabled={selectedMode === MapViewType.HEAT}
                              onclick={() => setSelectedMode(MapViewType.HEAT)} />
        </div>
    )
}

interface SelectButtonProps {
    text: String;
    disabled: boolean;
    onclick: () => void;
}

const SelectModeButton = ({text, disabled, onclick} : SelectButtonProps) => {
    return (
        <button type="button"
            onClick={onclick}
            disabled={disabled}>
                {text}
        </button>
    )
}