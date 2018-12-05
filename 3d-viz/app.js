/* global window */
import React, {Component} from 'react';
import {render} from 'react-dom';
import {InteractiveMap, StaticMap} from 'react-map-gl';
import DeckGL, {HexagonLayer, MapController, MapView, FirstPersonView, GridLayer, ScatterplotLayer} from 'deck.gl';

// Set your mapbox token here
const MAPBOX_TOKEN = 'pk.eyJ1IjoiZGFuZGk0OTAzIiwiYSI6ImNqcDZ1emFwbzE2d24zcHA3NjNtbTV6MzcifQ.SmvQNgtwXEgEwQVoeXXy3w';

const DATA_URL = 'http://localhost:5000/data';
const DISTRICT_URL = 'http://localhost:5000/districts';
const CATEGORY_URL = 'http://localhost:5000/categories';

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
    lightsPosition: [-140.41, 37.74, 8000, -3.807751, 54.104682, 8000],
    ambientRatio: 0.4,
    diffuseRatio: 0.9,
    specularRatio: 0.9,
    lightsStrength: [0.8, 0.0, 0.8, 0.0],
    numberOfLights: 2
};

const colorRange = [
    [200, 200, 100],
    [73, 227, 206],
    [216, 254, 181],
    [254, 255, 177],
    [254, 173, 84],
    [209, 55, 78]
];

const elevationScale = {min: 1, max: 50};

export class ControlPanel extends Component {
    constructor() {
        super();
        this.state = {
            gridSize: 0,
            categories: ['ALL'],
            startDate: '',
            endDate: ''
        };

        fetch(CATEGORY_URL, {method: 'GET', mode: 'cors'})
            .then(resp => resp.json())
            .then(cat => this.setState({categories: ['ALL', ...cat]}));
    }

    handleGridSizeChange(e) {
        this.setState({
            gridSize: e.target.value
        });
    }

    handleGridSizeMouseUp(e) {
        this.props.onGridSizeChange(this.state.gridSize);
    }

    handleStartDateChange(e) {
        console.log("StartDate " + e.target.value);
        this.setState({startDate: e.target.value});
        let dateToken = e.target.value.split("-");
        let newDate = [dateToken[1], dateToken[2], dateToken[0]].join("/");
        this.props.onStartDateChange(newDate)
    }

    handleEndDateChange(e) {
        console.log("EndDate " + e.target.value);
        this.setState({endDate: e.target.value});
        let dateToken = e.target.value.split("-");
        let newDate = [dateToken[1], dateToken[2], dateToken[0]].join("/");
        this.props.onEndDateChange(newDate)
    }

    handleCategoryChange(e) {
        console.log(e.target.value);
        this.props.onCategoryChange(e.target.value)
    }

    handleViewModeChange(e) {
        const isDistrict = e.target.value === "1";
        this.props.onViewModeChange(isDistrict)
    }

    handleStartHour(e) {
        this.props.onStartHourChange(e.target.value);
    }

    handleEndHour(e) {
        this.props.onEndHourChange(e.target.value);
    }

    render() {
        const categoryOpts = this.state.categories.map((cat) => <option value={cat}>{cat}</option>);
        const {startDate, endDate} = this.state;
        const hours = Array.from(Array(24).keys()).map(h => <option value={h}>{h}</option>);

        return (
            <div className="card" style={{width: "25vw", filter: "opacity(60)"}}>
                <div className="card-body">
                    <span className="card-title"><b>San Francisco Crime Data</b></span>
                    <br/>
                    <form>
                        <div className="form-group row">
                            <label htmlFor="formControlRange" className="col-form-label col-sm-4">Cell Size</label>
                            <input type="range" className="form-control-range col-sm-8"
                                   id="formControlRange"
                                   onMouseUp={e => this.handleGridSizeMouseUp(e)}
                                   onChange={e => this.handleGridSizeChange(e)}/>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="viewMode" className="col-form-label col-sm-4">Category</label>
                            <select id="viewMode" className="col-sm-8 form-control"
                                    onChangeCapture={e => this.handleCategoryChange(e)}>
                                {categoryOpts}
                            </select>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="categoryDropDown" className="col-form-label col-sm-4">View</label>
                            <select id="categoryDropdown" className="col-sm-8 form-control"
                                    onChangeCapture={e => this.handleViewModeChange(e)}>
                                <option value="0">Crime Occurences</option>
                                <option value="1">By District</option>
                            </select>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="startDate" className="col-form-label col-sm-4">Start Date</label>
                            <input className="col-sm-8 form-control" type="date" id="startDate" min="2000-01-01" max={endDate}
                                   onChangeCapture={e => this.handleStartDateChange(e)}/>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="endDate" className="col-form-label col-sm-4">End Date</label>
                            <input className="col-sm-8 form-control" type="date" id="endDate" min={startDate} max="2018-12-31"
                                   onChangeCapture={e => this.handleEndDateChange(e)}/>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="viewMode" className="col-form-label col-sm-4">Start Hour</label>
                            <select id="viewMode" className="col-sm-8 form-control"
                                    onChangeCapture={e => this.handleStartHour(e)}>
                                {hours}
                            </select>
                        </div>

                        <div className="form-group row">
                            <label htmlFor="viewMode" className="col-form-label col-sm-4">End Hour</label>
                            <select id="viewMode" className="col-sm-8 form-control"
                                    onChangeCapture={e => this.handleEndHour(e)}>
                                {hours}
                            </select>
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
            data: [],
            districts: [],
            radius: 50,
            upperPercentile: 100,
            coverage: 1,
            gridSize: 50,
            isDistrict: false,

            category: 'ALL',
            startDate: '1/1/2018',
            endDate: '2/2/2018',
            startHour: '0',
            endHour: '23'
        };

        this.startAnimationTimer = null;
        this.intervalTimer = null;

        this._startAnimate = this._startAnimate.bind(this);
        this._animateHeight = this._animateHeight.bind(this);

        fetch(DATA_URL, {method: 'GET', mode: 'cors'})
            .then(resp => resp.json())
            .then(data => this.setState({data: data}));

        fetch(DISTRICT_URL, {method: 'GET', mode: 'cors'})
            .then(resp => resp.json())
            .then(data => this.setState({districts: data}))
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

    componentWillUpdate(nextProps, nextState, nextContext) {
        if (this.state.category !== nextState.category ||
            this.state.startDate !== nextState.startDate ||
            this.state.endDate !== nextState.endDate) {

            let PARAMS = "cat=" + encodeURIComponent(nextState.category);
            PARAMS += "&startDate=" + encodeURIComponent(nextState.startDate);
            PARAMS += "&endDate=" + encodeURIComponent(nextState.endDate);

            if (!this.state.isDistrict) {
                const D_URL = DATA_URL + "?" + PARAMS;
                fetch(D_URL, {method: 'GET', mode: 'cors'})
                    .then(resp => resp.json())
                    .then(data => this.setState({data: data}))
            } else {
                const D_URL = DISTRICT_URL + "?" + PARAMS;
                console.log(D_URL);
                fetch(D_URL, {method: 'GET', mode: 'cors'})
                    .then(resp => resp.json())
                    .then(data => this.setState({districts: data}))
            }

        }
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

    static colorLookup(val) {
        val /= 400;
        val = Math.min(Math.floor(val), 5);
        const colors = [[0, 255, 0], [140, 255, 0], [255, 255, 0], [255, 200, 0], [255, 140, 0], [255, 0, 0]];
        return colors[val]
    }

    _renderLayers() {
        const {data, districts, radius, upperPercentile, coverage, gridSize} = this.state;

        if (!this.state.isDistrict) {
            return [
                new GridLayer({
                    id: 'heatmap',
                    cellSize: gridSize,
                    //colorRange: colorRange,
                    coverage: coverage,
                    data: data,
                    elevationRange: [0, 50],
                    elevationScale: this.state.elevationScale,
                    extruded: true,
                    getPosition: d => d,
                    lightSettings: LIGHT_SETTINGS,
                    onHover: this.props.onHover,
                    opacity: 1,
                    pickable: Boolean(this.props.onHover),
                    radius: gridSize,
                    upperPercentile: upperPercentile,
                })
            ];
        } else {
            return [
                new ScatterplotLayer({
                    id: 'district',
                    data: districts,
                    pickable: true,
                    opacity: 0.8,
                    radiusScale: 6,
                    radiusMinPixels: 1,
                    radiusMaxPixels: 250,
                    getPosition: d => (d.c),
                    getRadius: d => Math.sqrt(d.o) * 1.5,
                    getColor: d => App.colorLookup(Math.min(d.o, 2000))
                })
            ];
        }
    }

    onSizeChange(val) {
        this.setState({gridSize: (val + 1) * 1 / 5});
        console.log(this.state.gridSize)
    }

    onCategoryChange(cat) {
        this.setState({category: cat})
    }

    onStartDateChange(date) {
        this.setState({startDate: date})
    }

    onEndDateChange(date) {
        this.setState({endDate: date})
    }

    onViewModeChange(isDistrict) {
        this.setState({isDistrict: isDistrict});
        console.log(this.state.districts)
    }

    onStartHourChange(h) {
        this.setState({startHour: h})
    }

    onEndHourChange(h) {
        this.setState({endHour: h})
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

                <div style={{position: 'absolute', top: 20, right: 20}}>
                    <ControlPanel onGridSizeChange={val => this.onSizeChange(val)}
                                  onCategoryChange={cat => this.onCategoryChange(cat)}
                                  onStartDateChange={date => this.onStartDateChange(date)}
                                  onEndDateChange={date => this.onEndDateChange(date)}
                                  onViewModeChange={dis => this.onViewModeChange(dis)}
                                  onStartHourChange={dis => this.onStartHourChange(dis)}
                                  onEndHourChange={dis => this.onEndHourChange(dis)}/>
                </div>
            </div>
        );
    }
}

export function renderToDOM(container) {
    render(<App/>, container);

}
