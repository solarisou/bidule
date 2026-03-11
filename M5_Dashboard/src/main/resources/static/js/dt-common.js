/* =========================================================
   dt-common.js – Configuration DataTables partagée
   NavalCraft Dashboard
   ========================================================= */

const LANG_FR = {
    emptyTable:    "Aucune donnée disponible",
    info:          "Affichage de _START_ à _END_ sur _TOTAL_ entrées",
    infoEmpty:     "Affichage de 0 à 0 sur 0 entrée",
    infoFiltered:  "(filtré depuis _MAX_ entrées au total)",
    lengthMenu:    "Afficher _MENU_ entrées",
    loadingRecords:"Chargement...",
    search:        "Rechercher :",
    searchPlaceholder: "Recherche globale...",
    zeroRecords:   "Aucun résultat trouvé",
    paginate: { first: "«", last: "»", next: "›", previous: "‹" }
};

const DT_LENGTH_MENU = [[10, 25, 50, 100, -1], ["10", "25", "50", "100", "Tous"]];

/**
 * Initialise les filtres par colonne dans le tfoot d'un tableau DataTables.
 * @param {object} dt        – instance DataTables déjà initialisée
 * @param {number[]} selectCols – indices des colonnes à rendre en <select>
 */
function addColumnSearch(dt, selectCols) {
    dt.columns().every(function (i) {
        var col     = this;
        var $footer = $(col.footer());
        var header  = $(col.header()).text().trim();

        $footer.empty();

        if (selectCols.includes(i)) {
            /* ---- liste déroulante pour colonnes à valeurs discrètes ---- */
            var $sel = $('<select class="form-select form-select-sm dt-col-filter">'
                       + '<option value="">— Tous —</option></select>')
                .appendTo($footer)
                .on("change", function () {
                    var val = $.fn.dataTable.util.escapeRegex($(this).val());
                    col.search(val ? "^" + val + "$" : "", true, false).draw();
                });

            col.data().unique().sort().each(function (d) {
                var txt = String(d || "").trim();
                if (txt) $sel.append('<option value="' + txt + '">' + txt + '</option>');
            });
        } else {
            /* ---- champ texte pour les autres colonnes ---- */
            $('<input type="text" class="form-control form-control-sm dt-col-filter"'
                + ' placeholder="' + header + '">')
                .appendTo($footer)
                .on("input", function () {
                    col.search(this.value).draw();
                });
        }
    });
}
