# -*- coding: utf-8 -*-

from .api import get_price


class PlotUtils:

    def plot_candle(i, sdate='', edate='', dsh=False, vol=True):

        import plotly.graph_objs as go
        '''
            Plot candles of i
            Input: id
            Output: plotting data and default layout
        '''
        layout = go.Layout(
            barmode = 'stack',
            xaxis = dict(
                        type = 'category',
                        rangeslider = dict(visible=False),
                        spikemode = 'toaxis+across+marker',
                        ),
        )
        
        dt = []
        icolor, dcolor = 'red', 'green'
        _, n, v = get_price(i, sdate=sdate, edate=edate)
        v = (v - v.low.min()) * 10 / (v.high.max() - v.low.min()) + 2
        v = v.round(3) # compress html data
        
        dt += [
                go.Candlestick(
                        x=v.index.to_series(),
                        open=v.open,
                        high=v.high,
                        low=v.low,
                        close=v.close,
                        opacity=0.5,
                        hoverinfo='none',
                        name=n,
                        increasing={'line':{'color':icolor}},
                        decreasing={'line':{'color':dcolor}}
                        ),
            ]
        
        if vol:
            vvol = v.vol / v.vol.max()
            vvol *= 2
            dt += [go.Bar(x = v.index.to_series(), y=vvol, name='vol', opacity=0.5)]
            
        icolor, dcolor = 'cyan', 'gray'
        if dsh:
            _, _, dsh = get_price('sh000001', sdate=sdate, edate=edate)
            dsh = (dsh / dsh.iloc[0,0] - 1) * 10
            dt += [
                    go.Candlestick(
                            x=dsh.index.to_series(),
                            open=dsh.open,
                            high=dsh.high,
                            low=dsh.low,
                            close=dsh.close,
                            opacity=0.5,
                            hoverinfo='none',
                            name='sh',
                            increasing={'line':{'color':icolor}},
                            decreasing={'line':{'color':dcolor}}
                            ),
                ]
            
        return dt, layout

