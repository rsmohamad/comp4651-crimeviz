/* global window */
import React, {Component} from 'react';
import {render} from 'react-dom';
import {InteractiveMap, StaticMap} from 'react-map-gl';
import DeckGL, {HexagonLayer, MapController, MapView, FirstPersonView, GridLayer} from 'deck.gl';

// Set your mapbox token here
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZGFuZGk0OTAzIiwiYSI6ImNqcDZ1emFwbzE2d24zcHA3NjNtbTV6MzcifQ.SmvQNgtwXEgEwQVoeXXy3w'; // eslint-disable-line

// Source data CSV
//const DATA_URL =
//    'https://raw.githubusercontent.com/uber-common/deck.gl-data/master/examples/3d-heatmap/heatmap-data.csv'; // eslint-disable-line

const DATA_URL = 'http://localhost:5000/data';

export const INITIAL_VIEW_STATE = {
    longitude: -122.41,
    latitude: 37.74,
    zoom: 12,
    minZoom: 5,
    maxZoom: 15,
    pitch: 45,
    bearing: 45
};

const LIGHT_SETTINGS = {
    lightsPosition: [-0.144528, 49.739968, 8000, -3.807751, 54.104682, 8000],
    ambientRatio: 0.4,
    diffuseRatio: 0.8,
    specularRatio: 0.5,
    lightsStrength: [0.8, 0.0, 0.8, 0.0],
    numberOfLights: 2
};

const colorRange = [
    [1, 152, 189],
    [73, 227, 206],
    [216, 254, 181],
    [254, 237, 177],
    [254, 173, 84],
    [209, 55, 78]
];

const elevationScale = {min: 1, max: 50};

export class ControlPanel extends Component {
    constructor() {
        super();
        this.state = {
            gridSize: 0
        }
    }

    handleGridSizeChange(e) {
        this.setState({
            gridSize: e.target.value
        });
    }

    handleGridSizeMouseUp(e) {
        this.props.onGridSizeChange(this.state.gridSize);
    }

    render() {
        let cat = [];

        return (
            <div className="card rounded-0" style={{width: "25vw"}}>
                <div className="card-body">
                    <span className="card-title"><b>San Francisco Crime Data</b></span>
                    <br/>
                    <form>
                        <div className="form-group row">
                            <label htmlFor="formControlRange" className="col-form-label col-sm-4">Cell Size</label>
                            <input type="range" className="form-control-range col-sm-8" id="formControlRange"
                                   onMouseUp={e => this.handleGridSizeMouseUp(e)}
                                   onChange={e => this.handleGridSizeChange(e)}/>
                        </div>
                        <div className="form-group row">
                            <div className="dropdown col-sm-12">
                                <select>

                                    <option value="volvo">Volvo</option>
                                    <option value="saab">Saab</option>
                                    <option value="mercedes">Mercedes</option>
                                    <option value="audi">Audi</option>
                                </select>
                            </div>
                        </div>
                    </form>
                </div>
            </div>
        );
    }
}

/* eslint-disable react/no-deprecated */
export class App extends Component {
    static get defaultColorRange() {
        return colorRange;
    }

    constructor(props) {
        super(props);
        this.state = {
            elevationScale: elevationScale.min,
            gridSize: 50
        };

        this.startAnimationTimer = null;
        this.intervalTimer = null;

        this._startAnimate = this._startAnimate.bind(this);
        this._animateHeight = this._animateHeight.bind(this);
    }

    componentDidMount() {
        this._animate();
    }

    componentWillReceiveProps(nextProps) {
        if (nextProps.data && this.props.data && nextProps.data.length !== this.props.data.length) {
            this._animate();
        }
    }

    componentWillUnmount() {
        this._stopAnimate();
    }

    _animate() {
        this._stopAnimate();

        // wait 1.5 secs to start animation so that all data are loaded
        this.startAnimationTimer = window.setTimeout(this._startAnimate, 1500);
    }

    _startAnimate() {
        this.intervalTimer = window.setInterval(this._animateHeight, 20);
    }

    _stopAnimate() {
        window.clearTimeout(this.startAnimationTimer);
        window.clearTimeout(this.intervalTimer);
    }

    _animateHeight() {
        if (this.state.elevationScale === elevationScale.max) {
            this._stopAnimate();
        } else {
            this.setState({elevationScale: this.state.elevationScale + 1});
        }
    }

    _renderLayers() {
        const {data, radius = 50, upperPercentile = 100, coverage = 1} = this.props;
        const {gridSize} = this.state;

        return [
            new HexagonLayer({
                id: 'heatmap',
                colorRange: colorRange,
                coverage: coverage,
                data: data,
                elevationRange: [0, 100],
                elevationScale: this.state.elevationScale,
                extruded: true,
                getPosition: d => d.COORDINATE,
                lightSettings: LIGHT_SETTINGS,
                onHover: this.props.onHover,
                opacity: 1,
                pickable: Boolean(this.props.onHover),
                radius: gridSize,
                upperPercentile: upperPercentile,
            })
        ];
    }

    onSizeChange(val) {
        this.setState({
            gridSize: (val + 1) * 1/5
        });
        console.log(this.state.gridSize)
    }

    render() {
        const {viewState, controller = {dragRotate: true}, baseMap = true} = this.props;

        return (
            <div>
                <DeckGL
                    layers={this._renderLayers()}
                    initialViewState={INITIAL_VIEW_STATE}
                    controller={true}
                >

                    <MapView viewState={viewState} controller={true}>
                        <InteractiveMap
                            reuseMaps
                            mapStyle="mapbox://styles/mapbox/dark-v9"
                            preventStyleDiffing={true}
                            mapboxApiAccessToken={MAPBOX_TOKEN}
                        />
                    </MapView>

                </DeckGL>

                <div style={{position: 'absolute', top: 10, right: 10}}>
                    <ControlPanel onGridSizeChange={val => this.onSizeChange(val)}/>
                </div>
            </div>
        );
    }
}

export function renderToDOM(container) {
    render(<App/>, container);

    fetch(DATA_URL, {method: 'GET', mode: 'cors'})
        .then(resp => resp.json())
        .then(data => render(<App data={data}/>, container))
}
