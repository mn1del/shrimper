from flask import Blueprint, redirect, render_template, url_for


bp = Blueprint('main', __name__)


@bp.route('/')
def index():
    return redirect(url_for('main.series_index'))


@bp.route('/race-sheets')
def series_index():
    return render_template('race_sheets.html', title='Series Index')


@bp.route('/series/new')
def series_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Series', None)]
    return render_template('series_form.html', title='Create New Series', breadcrumbs=breadcrumbs)


@bp.route('/races/new')
def race_new():
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), ('Create New Race', None)]
    return render_template('race_form.html', title='Create New Race', breadcrumbs=breadcrumbs)


@bp.route('/series/<int:series_id>')
def series_detail(series_id):
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), (f'Series {series_id}', None)]
    return render_template('series_detail.html', title=f'Series {series_id}', breadcrumbs=breadcrumbs)


@bp.route('/races/<int:race_id>')
def race_sheet(race_id):
    breadcrumbs = [('Race Sheets', url_for('main.series_index')), (f'Race {race_id}', None)]
    return render_template('race_sheet.html', title=f'Race {race_id}', breadcrumbs=breadcrumbs)


@bp.route('/standings/traditional')
def standings_traditional():
    breadcrumbs = [('Standings', None), ('Traditional', None)]
    return render_template('standings_traditional.html', title='Traditional Standings', breadcrumbs=breadcrumbs)


@bp.route('/standings/league')
def standings_league():
    breadcrumbs = [('Standings', None), ('League', None)]
    return render_template('standings_league.html', title='League Standings', breadcrumbs=breadcrumbs)


@bp.route('/fleet')
def fleet():
    breadcrumbs = [('Fleet', None)]
    return render_template('fleet.html', title='Fleet', breadcrumbs=breadcrumbs)


@bp.route('/rules')
def rules():
    breadcrumbs = [('Rules', None)]
    return render_template('rules.html', title='Rules', breadcrumbs=breadcrumbs)


@bp.route('/settings')
def settings():
    breadcrumbs = [('Settings', None)]
    return render_template('settings.html', title='Settings', breadcrumbs=breadcrumbs)
