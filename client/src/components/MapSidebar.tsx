export enum MapViewType {
    STOPS = 0
}


export interface SidebarProps {
    selectedMode: MapViewType;
    setSelectedMode: React.Dispatch<React.SetStateAction<MapViewType>>;
}

export const MapSidebar = ({selectedMode, setSelectedMode} : SidebarProps) => {

    return (
        <div id="map-sidebar">
            <button type="button"
                onClick={() => setSelectedMode(MapViewType.STOPS)}
                data-selected={selectedMode.valueOf() === MapViewType.STOPS}>
                    Truck Stops
            </button>
        </div>
    )
}